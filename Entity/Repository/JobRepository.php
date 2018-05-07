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

namespace JMS\JobQueueBundle\Entity\Repository;

use Doctrine\DBAL\Types\Type;
use Doctrine\ORM\EntityRepository;
use JMS\JobQueueBundle\Entity\Job;
use Doctrine\DBAL\Connection;

class JobRepository extends EntityRepository
{
    /**
     * @param       $command
     * @param array $args
     *
     * @return mixed
     * @throws \Doctrine\ORM\NonUniqueResultException
     */
    public function findJob($command, array $args = [])
    {
        return $this->_em->createQueryBuilder()
            ->select('j')
            ->from('JMSJobQueueBundle:Job', 'j')
            ->where('j.command = :command')
            ->andWhere('j.args = :args')
            ->setParameter('command', $command)
            ->setParameter('args', $args, Type::JSON_ARRAY)
            ->setMaxResults(1)
            ->getQuery()
            ->getOneOrNullResult()
            ;
    }

    /**
     * @param       $command
     * @param array $args
     *
     * @return Job|mixed
     * @throws \Doctrine\ORM\NoResultException
     * @throws \Doctrine\ORM\NonUniqueResultException
     */
    public function getOrCreateIfNotExists($command, array $args = [])
    {
        if (null !== $job = $this->findJob($command, $args)) {
            return $job;
        }

        $job = new Job($command, $args, false);
        $this->_em->persist($job);
        $this->_em->flush($job);

        $firstJob = $this->_em->createQueryBuilder()
            ->select('j')
            ->from('JMSJobQueueBundle:Job', 'j')
            ->where('j.command = :command')
            ->andWhere('j.args = :args')
            ->orderBy('j.id', 'asc')
            ->setParameter('command', $command)
            ->setParameter('args', $args, 'json_array')
            ->setMaxResults(1)
            ->getQuery()
            ->getSingleResult()
        ;

        if ($firstJob === $job) {
            $job->setState(Job::STATE_PENDING);
            $this->_em->persist($job);
            $this->_em->flush($job);

            return $job;
        }

        $this->_em->remove($job);
        $this->_em->flush($job);

        return $firstJob;
    }

    /**
     * @param array $excludedIds
     * @param array $excludedQueues
     * @param array $restrictedQueues
     *
     * @return mixed|null
     * @throws \Doctrine\ORM\NonUniqueResultException
     */
    public function findStartableJob(array &$excludedIds = [], $excludedQueues = [], $restrictedQueues = [])
    {
        while (null !== $job = $this->findPendingJob($excludedIds, $excludedQueues, $restrictedQueues)) {
            if ($job->isStartable()) {
                return $job;
            }

            $excludedIds[] = $job->getId();

            // We do not want to have non-startable jobs floating around in
            // cache as they might be changed by another process. So, better
            // re-fetch them when they are not excluded anymore.
            $this->_em->detach($job);
        }

        return null;
    }

    /**
     * @param array $jobQueues
     *
     * @return mixed
     */
    public function findRunningJobs($jobQueues = [])
    {
        $qb = $this->_em->createQueryBuilder()
            ->select('j')
            ->from('JMSJobQueueBundle:Job', 'j')
            ->where('j.state = :state')
            ->setParameter('state', Job::STATE_RUNNING)
        ;

        if (!empty($jobQueues)) {
            $qb->andWhere('j.queue IN (:queues)')->setParameter('queues', $jobQueues);
        }

        return $qb->getQuery()->getResult();
    }

    /**
     * @param array $excludedIds
     * @param array $excludedQueues
     * @param array $restrictedQueues
     *
     * @return mixed
     * @throws \Doctrine\ORM\NonUniqueResultException
     */
    private function findPendingJob(array $excludedIds = [], array $excludedQueues = [], array $restrictedQueues = [])
    {
        $qb = $this->_em->createQueryBuilder()
            ->select('j')
            ->from('JMSJobQueueBundle:Job', 'j')
            ->leftJoin('j.dependencies', 'd')
            ->orderBy('j.priority', 'ASC')
            ->addOrderBy('j.id', 'ASC')
        ;

        $conditions = [];

        $conditions[] = $qb->expr()->lt('j.executeAfter', ':now');
        $qb->setParameter(':now', new \DateTime(), 'datetime');

        $conditions[] = $qb->expr()->eq('j.state', ':state');
        $qb->setParameter('state', Job::STATE_PENDING);

        if (!empty($excludedIds)) {
            $conditions[] = $qb->expr()->notIn('j.id', ':excludedIds');
            $qb->setParameter('excludedIds', $excludedIds, Connection::PARAM_INT_ARRAY);
        }

        if (!empty($excludedQueues)) {
            $conditions[] = $qb->expr()->notIn('j.queue', ':excludedQueues');
            $qb->setParameter('excludedQueues', $excludedQueues, Connection::PARAM_STR_ARRAY);
        }

        if (!empty($restrictedQueues)) {
            $conditions[] = $qb->expr()->in('j.queue', ':restrictedQueues');
            $qb->setParameter('restrictedQueues', $restrictedQueues, Connection::PARAM_STR_ARRAY);
        }

        $qb->where(call_user_func_array([$qb->expr(), 'andX'], $conditions));

        return $qb->getQuery()->setMaxResults(1)->getOneOrNullResult();
    }
}
