<?php

namespace JMS\JobQueueBundle\Command;

use Doctrine\ORM\EntityManagerInterface;
use JMS\JobQueueBundle\Entity\Job;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;

class CleanUpCommand extends Command
{
    /** @var string */
    protected static $defaultName = 'jms-job-queue:clean-up';

    /** @var \Doctrine\ORM\EntityManagerInterface */
    private $entityManager;

    /**
     * RunCommand constructor.
     *
     * @param EntityManagerInterface $entityManager
     */
    public function __construct(EntityManagerInterface $entityManager)
    {
        parent::__construct(self::$defaultName);
        $this->entityManager = $entityManager;
    }

    protected function configure()
    {
        $this
            ->setName(self::$defaultName)
            ->setDescription('Cleans up jobs which exceed the maximum retention time.')
            ->addOption(
                'max-retention',
                null,
                InputOption::VALUE_REQUIRED,
                'The maximum retention time (value must be parsable by DateTime).',
                '30 days'
            )
        ;
    }

    /**
     * @param InputInterface  $input
     * @param OutputInterface $output
     *
     * @return int|null|void
     * @throws \Doctrine\DBAL\DBALException
     */
    protected function execute(InputInterface $input, OutputInterface $output)
    {
        $conn = $this->entityManager->getConnection();
        $jobs = $this->entityManager->createQuery(
            "SELECT j FROM JMSJobQueueBundle:Job j WHERE j.closedAt < :maxRetentionTime AND j.originalJob IS NULL"
        )
            ->setParameter('maxRetentionTime', new \DateTime('-' . $input->getOption('max-retention')))
            ->setMaxResults(1000)
            ->getResult()
        ;

        $incomingDepsSql = $conn->getDatabasePlatform()->modifyLimitQuery(
            "SELECT 1 FROM jms_job_dependencies WHERE dest_job_id = :id",
            1
        )
        ;

        foreach ($jobs as $job) {
            /** @var Job $job */

            $result = $conn->executeQuery($incomingDepsSql, ['id' => $job->getId()]);
            if ($result->fetchColumn() !== false) {
                // There are still other jobs that depend on this, we will come back later.
                continue;
            }

            $this->entityManager->remove($job);
        }

        $this->entityManager->flush();
    }
}
