<?php

namespace JMS\JobQueueBundle\Command;

use JMS\JobQueueBundle\Entity\Job;
use JMS\JobQueueBundle\Entity\Repository\JobManager;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputArgument;

class MarkJobIncompleteCommand extends Command
{
    /** @var JobManager */
    private $jobManager;

    /** @var \Doctrine\ORM\EntityManagerInterface */
    private $entityManager;

    /** @var string */
    protected static $defaultName = 'jms-job-queue:mark-incomplete';

    /**
     * RunCommand constructor.
     *
     * @param JobManager $jobManager
     */
    public function __construct(JobManager $jobManager)
    {
        parent::__construct(self::$defaultName);
        $this->jobManager = $jobManager;
        $this->entityManager = $jobManager->getEntityManager();
    }

    protected function configure()
    {
        $this
            ->setName(self::$defaultName)
            ->setDescription('Internal command (do not use). It marks jobs as incomplete.')
            ->addArgument('job-id', InputArgument::REQUIRED, 'The ID of the Job.')
        ;
    }

    /**
     * @param InputInterface  $input
     * @param OutputInterface $output
     *
     * @return int|null|void
     * @throws \Doctrine\DBAL\ConnectionException
     * @throws \Exception
     */
    protected function execute(InputInterface $input, OutputInterface $output)
    {
        /** @var Job $job */
        $job = $this->entityManager->find('JMSJobQueueBundle:Job', $input->getArgument('job-id'));
        $this->jobManager->closeJob(
            $job,
            Job::STATE_INCOMPLETE
        );
    }
}
