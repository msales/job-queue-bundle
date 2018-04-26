<?php
namespace JMS\JobQueueBundle\Entity\Repository;

use Doctrine\Common\Persistence\ObjectRepository;
use Doctrine\ORM\EntityManagerInterface;
use JMS\JobQueueBundle\Entity\Job;
use JMS\JobQueueBundle\Event\StateChangeEvent;
use Symfony\Component\EventDispatcher\EventDispatcher;
use Symfony\Component\EventDispatcher\EventDispatcherInterface;

class JobManager
{
    /**
     * @var ObjectRepository
     */
    private $repository;

    /**
     * @var EventDispatcherInterface
     */
    private $eventDispatcher;

    /**
     * @var EntityManagerInterface
     */
    private $entityManager;

    /**
     * JobRepository constructor.
     *
     * @param JobRepository            $repository
     * @param EntityManagerInterface   $entityManager
     * @param EventDispatcherInterface $eventDispatcher
     */
    public function __construct(JobRepository $repository, EntityManagerInterface $entityManager, EventDispatcherInterface $eventDispatcher)
    {
        $this->repository = $repository;
        $this->eventDispatcher = $eventDispatcher;
        $this->entityManager = $entityManager;
    }

    /**
     * @param       $command
     * @param array $args
     *
     * @return mixed
     * @throws \Doctrine\ORM\NonUniqueResultException
     */
    public function getJob($command, array $args = [])
    {
        if (null !== $job = $this->repository->findJob($command, $args)) {
            return $job;
        }

        throw new \RuntimeException(
            sprintf('Found no job for command "%s" with args "%s".', $command, json_encode($args))
        );
    }

    /**
     * @param Job $job
     * @param     $finalState
     *
     * @throws \Doctrine\DBAL\ConnectionException
     * @throws \Exception
     */
    public function closeJob(Job $job, $finalState)
    {
        $this->entityManager->getConnection()->beginTransaction();
        try {
            $visited = [];
            $this->closeJobInternal($job, $finalState, $visited);
            $this->entityManager->flush();
            $this->entityManager->getConnection()->commit();

            // Clean-up entity manager to allow for garbage collection to kick in.
            foreach ($visited as $job) {
                // If the job is an original job which is now being retried, let's
                // not remove it just yet.
                if (!$job->isClosedNonSuccessful() || $job->isRetryJob()) {
                    continue;
                }

                $this->entityManager->detach($job);
            }
        } catch (\Exception $ex) {
            $this->entityManager->getConnection()->rollback();

            throw $ex;
        }
    }

    /**
     * @param Job   $job
     * @param       $finalState
     * @param array $visited
     */
    private function closeJobInternal(Job $job, $finalState, array &$visited = [])
    {
        if (in_array($job, $visited, true)) {
            return;
        }
        $visited[] = $job;

        if ($job->isRetryJob() || 0 === count($job->getRetryJobs())) {
            $event = new StateChangeEvent($job, $finalState);
            $this->eventDispatcher->dispatch('jms_job_queue.job_state_change', $event);
            $finalState = $event->getNewState();
        }

        switch ($finalState) {
            case Job::STATE_CANCELED:
                $job->setState(Job::STATE_CANCELED);
                $this->entityManager->persist($job);

                if ($job->isRetryJob()) {
                    $this->closeJobInternal($job->getOriginalJob(), Job::STATE_CANCELED, $visited);

                    return;
                }

                return;

            case Job::STATE_FAILED:
            case Job::STATE_TERMINATED:
            case Job::STATE_INCOMPLETE:
                if ($job->isRetryJob()) {
                    $job->setState($finalState);
                    $this->entityManager->persist($job);

                    $this->closeJobInternal($job->getOriginalJob(), $finalState);

                    return;
                }

                // The original job has failed, and we are allowed to retry it.
                if ($job->isRetryAllowed()) {
                    $retryJob = new Job($job->getCommand(), $job->getArgs());
                    $retryJob->setMaxRuntime($job->getMaxRuntime());
                    $retryJob->setExecuteAfter(new \DateTime('+' . (pow(5, count($job->getRetryJobs()))) . ' seconds'));

                    $job->addRetryJob($retryJob);
                    $this->entityManager->persist($retryJob);
                    $this->entityManager->persist($job);

                    return;
                }

                $job->setState($finalState);
                $this->entityManager->persist($job);

                return;

            case Job::STATE_FINISHED:
                if ($job->isRetryJob()) {
                    $job->getOriginalJob()->setState($finalState);
                    $this->entityManager->persist($job->getOriginalJob());
                }
                $job->setState($finalState);
                $this->entityManager->persist($job);

                return;

            default:
                throw new \LogicException(sprintf('Non allowed state "%s" in closeJobInternal().', $finalState));
        }
    }

    /**
     * @return EntityManagerInterface
     */
    public function getEntityManager()
    {
        return $this->entityManager;
    }

    /**
     * @return ObjectRepository|JobRepository
     */
    public function getJobRepository()
    {
        return $this->repository;
    }

    /**
     * @return EventDispatcherInterface
     */
    public function getDispatcher()
    {
        return $this->eventDispatcher;
    }
}
