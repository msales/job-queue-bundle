<?php

/*
 * Copyright 2012 Johannes M. Schmitt <schmittjoh@gmail.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

namespace JMS\JobQueueBundle\Command;

use Doctrine\ORM\EntityManagerInterface;
use JMS\JobQueueBundle\Entity\Repository\JobManager;
use JMS\JobQueueBundle\Entity\Repository\JobRepository;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Event\ConsoleCommandEvent;
use Symfony\Component\EventDispatcher\EventDispatcher;
use Symfony\Component\Process\Exception\ProcessFailedException;
use JMS\JobQueueBundle\Exception\InvalidArgumentException;
use JMS\JobQueueBundle\Event\NewOutputEvent;
use Symfony\Component\Process\Process;
use JMS\JobQueueBundle\Entity\Job;
use JMS\JobQueueBundle\Event\StateChangeEvent;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Output\OutputInterface;

class RunCommand extends Command
{
    /** @var string */
    protected static $defaultName = 'jms-job-queue:run';

    /** @var string */
    private $env;

    /** @var boolean */
    private $verbose;

    /** @var OutputInterface */
    private $output;

    /** @var InputInterface */
    private $input;

    /** @var EventDispatcher */
    private $dispatcher;

    /** @var array */
    private $runningJobs = [];

    /** @var array */
    private $restrictedQueues = [];

    /** @var EntityManagerInterface|EntityManagerInterface */
    private $entityManager;

    /** @var JobRepository */
    private $jobRepository;

    /** @var JobManager */
    private $jobManager;

    /** @var mixed */
    private $queueOptionsDefault;

    /** @var mixed */
    private $queueOptions;

    /** @var string */
    private $kernelDir;

    /**
     * RunCommand constructor.
     *
     * @param JobManager $jobManager
     * @param mixed      $queueOptionsDefault
     * @param mixed      $queueOptions
     * @param string     $kernelDir
     */
    public function __construct(JobManager $jobManager, $queueOptionsDefault, $queueOptions, $kernelDir)
    {
        parent::__construct(self::$defaultName);
        $this->jobManager = $jobManager;
        $this->entityManager = $jobManager->getEntityManager();
        $this->jobRepository = $jobManager->getJobRepository();
        $this->dispatcher = $jobManager->getDispatcher();
        $this->queueOptionsDefault = $queueOptionsDefault;
        $this->queueOptions = $queueOptions;
        $this->kernelDir = $kernelDir;
    }

    protected function configure()
    {
        $this
            ->setName(self::$defaultName)
            ->setDescription('Runs jobs from the queue.')
            ->addOption('max-runtime', 'r', InputOption::VALUE_REQUIRED, 'The maximum runtime in seconds.', 900)
            ->addOption(
                'max-concurrent-jobs',
                'j',
                InputOption::VALUE_REQUIRED,
                'The maximum number of concurrent jobs.',
                4
            )
            ->addOption(
                'idle-time',
                null,
                InputOption::VALUE_REQUIRED,
                'Time to sleep when the queue ran out of jobs.',
                2
            )
            ->addOption(
                'queue',
                null,
                InputOption::VALUE_OPTIONAL | InputOption::VALUE_IS_ARRAY,
                'Restrict to one or more queues.',
                []
            )
        ;
    }

    /**
     * @param InputInterface  $input
     * @param OutputInterface $output
     *
     * @return int|null|void
     * @throws \Doctrine\DBAL\ConnectionException
     * @throws \Doctrine\ORM\NonUniqueResultException
     * @throws \Exception
     */
    protected function execute(InputInterface $input, OutputInterface $output)
    {
        $startTime = time();

        $maxRuntime = (integer) $input->getOption('max-runtime');
        if ($maxRuntime <= 0) {
            throw new InvalidArgumentException('The maximum runtime must be greater than zero.');
        }

        $maxJobs = (integer) $input->getOption('max-concurrent-jobs');
        if ($maxJobs <= 0) {
            throw new InvalidArgumentException('The maximum number of jobs per queue must be greater than zero.');
        }

        $idleTime = (integer) $input->getOption('idle-time');
        if ($idleTime <= 0) {
            throw new InvalidArgumentException('Time to sleep when idling must be greater than zero.');
        }

        $this->restrictedQueues = $input->getOption('queue');

        $this->env = $input->getOption('env');
        $this->verbose = $input->getOption('verbose');
        $this->output = $output;
        $this->input = $input;
        $this->entityManager->getConnection()->getConfiguration()->setSQLLogger(null);

        $this->cleanUpStaleJobs();

        $this->runJobs(
            $startTime,
            $maxRuntime,
            $idleTime,
            $maxJobs,
            $this->queueOptionsDefault,
            $this->queueOptions
        );
    }

    /**
     * @param       $startTime
     * @param       $maxRuntime
     * @param       $idleTime
     * @param       $maxJobs
     * @param array $queueOptionsDefaults
     * @param array $queueOptions
     *
     * @throws \Doctrine\DBAL\ConnectionException
     * @throws \Doctrine\ORM\NonUniqueResultException
     * @throws \Exception
     */
    private function runJobs($startTime, $maxRuntime, $idleTime, $maxJobs, array $queueOptionsDefaults, array $queueOptions)
    {
        $waitTime = 1;
        while (true) {
            $this->checkRunningJobs();
            if (time() - $startTime > $maxRuntime) {
                if (empty($this->runningJobs)) {
                    return;
                }

                $waitTime = 5;
            }

            $this->startJobs($idleTime, $maxJobs, $queueOptionsDefaults, $queueOptions);
            $event = new ConsoleCommandEvent($this, $this->input, $this->output);
            $this->dispatcher->dispatch('jms_job_queue.run_jobs', $event);
            sleep($waitTime);
        }
    }

    /**
     * @param $idleTime
     * @param $maxJobs
     * @param $queueOptionsDefaults
     * @param $queueOptions
     *
     * @throws \Doctrine\DBAL\ConnectionException
     * @throws \Doctrine\ORM\NonUniqueResultException
     * @throws \Exception
     */
    private function startJobs($idleTime, $maxJobs, $queueOptionsDefaults, $queueOptions)
    {
        $excludedIds = [];
        while (count($this->runningJobs) < $maxJobs) {
            $pendingJob = $this->jobRepository->findStartableJob(
                $excludedIds,
                $this->getExcludedQueues($queueOptionsDefaults, $queueOptions, $maxJobs),
                $this->restrictedQueues
            );

            if (null === $pendingJob) {
                sleep($idleTime);

                return;
            }

            $this->startJob($pendingJob);
        }
    }

    private function getExcludedQueues(array $queueOptionsDefaults, array $queueOptions, $maxConcurrentJobs)
    {
        $excludedQueues = [];
        foreach ($this->getRunningJobsPerQueue() as $queue => $count) {
            if ($count >= $this->getMaxConcurrentJobs(
                    $queue,
                    $queueOptionsDefaults,
                    $queueOptions,
                    $maxConcurrentJobs
                )) {
                $excludedQueues[] = $queue;
            }
        }

        return $excludedQueues;
    }

    private function getMaxConcurrentJobs($queue, array $queueOptionsDefaults, array $queueOptions, $maxConcurrentJobs)
    {
        if (isset($queueOptions[$queue]['max_concurrent_jobs'])) {
            return (integer) $queueOptions[$queue]['max_concurrent_jobs'];
        }

        if (isset($queueOptionsDefaults['max_concurrent_jobs'])) {
            return (integer) $queueOptionsDefaults['max_concurrent_jobs'];
        }

        return $maxConcurrentJobs;
    }

    private function getRunningJobsPerQueue()
    {
        $runningJobsPerQueue = [];
        foreach ($this->runningJobs as $jobDetails) {
            /** @var Job $job */
            $job = $jobDetails['job'];

            $queue = $job->getQueue();
            if (!isset($runningJobsPerQueue[$queue])) {
                $runningJobsPerQueue[$queue] = 0;
            }
            $runningJobsPerQueue[$queue] += 1;
        }

        return $runningJobsPerQueue;
    }

    /**
     * @throws \Doctrine\DBAL\ConnectionException
     * @throws \Exception
     */
    private function checkRunningJobs()
    {
        foreach ($this->runningJobs as $i => &$data) {
            $newOutput = substr($data['process']->getOutput(), $data['output_pointer']);
            $data['output_pointer'] += strlen($newOutput);

            $newErrorOutput = substr($data['process']->getErrorOutput(), $data['error_output_pointer']);
            $data['error_output_pointer'] += strlen($newErrorOutput);

            if (!empty($newOutput)) {
                $event = new NewOutputEvent($data['job'], $newOutput, NewOutputEvent::TYPE_STDOUT);
                $this->dispatcher->dispatch('jms_job_queue.new_job_output', $event);
                $newOutput = $event->getNewOutput();
            }

            if (!empty($newErrorOutput)) {
                $event = new NewOutputEvent($data['job'], $newErrorOutput, NewOutputEvent::TYPE_STDERR);
                $this->dispatcher->dispatch('jms_job_queue.new_job_output', $event);
                $newErrorOutput = $event->getNewOutput();
            }

            if ($this->verbose) {
                if (!empty($newOutput)) {
                    $this->output->writeln(
                        'Job ' . $data['job']->getId() . ': ' . str_replace(
                            "\n",
                            "\nJob " . $data['job']->getId() . ": ",
                            $newOutput
                        )
                    );
                }

                if (!empty($newErrorOutput)) {
                    $this->output->writeln(
                        'Job ' . $data['job']->getId() . ': ' . str_replace(
                            "\n",
                            "\nJob " . $data['job']->getId() . ": ",
                            $newErrorOutput
                        )
                    );
                }
            }

            // Check whether this process exceeds the maximum runtime, and terminate if that is
            // the case.
            $runtime = time() - $data['job']->getStartedAt()->getTimestamp();
            if ($data['job']->getMaxRuntime() > 0 && $runtime > $data['job']->getMaxRuntime()) {
                $data['process']->stop(5);

                $this->output->writeln($data['job'] . ' terminated; maximum runtime exceeded.');
                $this->jobManager->closeJob($data['job'], Job::STATE_TERMINATED);
                unset($this->runningJobs[$i]);

                continue;
            }

            if ($data['process']->isRunning()) {
                // For long running processes, it is nice to update the output status regularly.
                $data['job']->addOutput($newOutput);
                $data['job']->addErrorOutput($newErrorOutput);
                $data['job']->checked();
                $em = $this->entityManager;
                $em->persist($data['job']);
                $em->flush($data['job']);

                continue;
            }

            $this->output->writeln($data['job'] . ' finished with exit code ' . $data['process']->getExitCode() . '.');

            // If the Job exited with an exception, let's reload it so that we
            // get access to the stack trace. This might be useful for listeners.
            $this->entityManager->refresh($data['job']);

            $data['job']->setExitCode($data['process']->getExitCode());
            $data['job']->setOutput($data['process']->getOutput());
            $data['job']->setErrorOutput($data['process']->getErrorOutput());
            $data['job']->setRuntime(time() - $data['start_time']);

            $newState = 0 === $data['process']->getExitCode() ? Job::STATE_FINISHED : Job::STATE_FAILED;
            $this->jobManager->closeJob($data['job'], $newState);
            unset($this->runningJobs[$i]);
        }

        gc_collect_cycles();
    }

    /**
     * @param Job $job
     *
     * @throws \Doctrine\DBAL\ConnectionException
     * @throws \Exception
     */
    private function startJob(Job $job)
    {
        $event = new StateChangeEvent($job, Job::STATE_RUNNING);
        $this->dispatcher->dispatch('jms_job_queue.job_state_change', $event);
        $newState = $event->getNewState();

        if (Job::STATE_CANCELED === $newState) {
            $this->jobManager->closeJob($job, Job::STATE_CANCELED);

            return;
        }

        if (Job::STATE_RUNNING !== $newState) {
            throw new \LogicException(sprintf('Unsupported new state "%s".', $newState));
        }

        $job->setState(Job::STATE_RUNNING);
        $em = $this->entityManager;
        $em->persist($job);
        $em->flush($job);

        $processlist = $this->buildProcessList() . ' ' . $job->getCommand() . ' --jms-job-id=' . $job->getId();

        foreach ($job->getArgs() as $arg) {
            $processlist .= ' ' . $arg;
        }

        $proc = new Process($processlist);
        $proc->start();
        $this->output->writeln(sprintf('Started %s.', $job));

        $this->runningJobs[] = [
            'process'              => $proc,
            'job'                  => $job,
            'start_time'           => time(),
            'output_pointer'       => 0,
            'error_output_pointer' => 0,
        ];
    }

    /**
     * Cleans up stale jobs.
     *
     * A stale job is a job where this command has exited with an error
     * condition. Although this command is very robust, there might be cases
     * where it might be terminated abruptly (like a PHP segfault, a SIGTERM signal, etc.).
     *
     * In such an error condition, these jobs are cleaned-up on restart of this command.
     */
    private function cleanUpStaleJobs()
    {
        $repo = $this->jobRepository;
        foreach ($repo->findRunningJobs($this->restrictedQueues) as $job) {
            // If the original job has retry jobs, then one of them is still in
            // running state. We can skip the original job here as it will be
            // processed automatically once the retry job is processed.
            if (!$job->isRetryJob() && count($job->getRetryJobs()) > 0) {
                continue;
            }

            $processlist = $this->buildProcessList() . ' jms-job-queue:mark-incomplete ' . $job->getId(
                ) . ' --env=' . $this->env . ' --verbose';
            // We use a separate process to clean up.

            $proc = new Process($processlist);
            if (0 !== $proc->run()) {
                $ex = new ProcessFailedException($proc);

                $this->output->writeln(
                    sprintf('There was an error when marking %s as incomplete: %s', $job, $ex->getMessage())
                );
            }
        }
    }

    /**
     * @return string
     */
    private function buildProcessList()
    {
        $processlist = '';
        // PHP wraps the process in "sh -c" by default, but we need to control
        // the process directly.
        if (!defined('PHP_WINDOWS_VERSION_MAJOR')) {
            $processlist .= ' exec';
        }

        $processlist .= ' php ' . $this->kernelDir . '/console ' . '--env=' . $this->env;

        if ($this->verbose) {
            $processlist .= ' --verbose';
        }

        return $processlist;
    }
}
