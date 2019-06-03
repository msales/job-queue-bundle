<?php

namespace JMS\JobQueueBundle\Tests\Functional;

use Doctrine\ORM\EntityManager;
use JMS\JobQueueBundle\Entity\Job;
use JMS\JobQueueBundle\Entity\Repository\JobRepository;
use JMS\JobQueueBundle\Tests\Functional\TestBundle\Entity\Train;
use JMS\JobQueueBundle\Tests\Functional\TestBundle\Entity\Wagon;

class JobRepositoryTest extends BaseTestCase
{
    /** @var EntityManager */
    private $em;

    /** @var JobRepository */
    private $repo;

    /**
     * @throws \Doctrine\ORM\NoResultException
     * @throws \Doctrine\ORM\NonUniqueResultException
     */
    public function getOrCreateIfNotExists()
    {
        $a = $this->repo->getOrCreateIfNotExists('a');
        $this->assertSame($a, $this->repo->getOrCreateIfNotExists('a'));
        $this->assertNotSame($a, $this->repo->getOrCreateIfNotExists('a', ['foo']));
    }

    /**
     * @throws \Doctrine\ORM\NonUniqueResultException
     * @throws \Doctrine\ORM\ORMException
     * @throws \Doctrine\ORM\OptimisticLockException
     */
    public function testFindStartableJob()
    {
        $this->assertNull($this->repo->findStartableJob());

        $a = new Job('a');
        $a->setState('running');
        $b = new Job('b');
        $c = new Job('c');
        $b->addDependency($c);
        $this->em->persist($a);
        $this->em->persist($b);
        $this->em->persist($c);
        $this->em->flush();

        $excludedIds = [];
        $this->assertSame($c, $this->repo->findStartableJob($excludedIds));
        $this->assertEquals([$b->getId()], $excludedIds);
    }

    /**
     * @throws \Doctrine\ORM\NonUniqueResultException
     * @throws \Doctrine\ORM\ORMException
     * @throws \Doctrine\ORM\OptimisticLockException
     */
    public function testFindStartableJobDetachesNonStartableJobs()
    {
        $a = new Job('a');
        $b = new Job('b');
        $a->addDependency($b);
        $this->em->persist($a);
        $this->em->persist($b);
        $this->em->flush();

        $this->assertTrue($this->em->contains($a));
        $this->assertTrue($this->em->contains($b));

        $excludedIds = [];
        $startableJob = $this->repo->findStartableJob($excludedIds);
        $this->assertNotNull($startableJob);
        $this->assertEquals($b->getId(), $startableJob->getId());
        $this->assertEquals([$a->getId()], $excludedIds);
        $this->assertFalse($this->em->contains($a));
        $this->assertTrue($this->em->contains($b));
    }

    /**
     * @throws \Doctrine\Common\Persistence\Mapping\MappingException
     * @throws \Doctrine\ORM\ORMException
     * @throws \Doctrine\ORM\OptimisticLockException
     * @throws \Doctrine\ORM\TransactionRequiredException
     */
    public function testModifyingRelatedEntity()
    {
        $wagon = new Wagon();
        $train = new Train();
        $wagon->train = $train;

        $defEm = self::$kernel->getContainer()->get('doctrine')->getManager('default');
        $defEm->persist($wagon);
        $defEm->persist($train);
        $defEm->flush();

        $j = new Job('j');
        $j->addRelatedEntity($wagon);
        $this->em->persist($j);
        $this->em->flush();

        $defEm->clear();
        $this->em->clear();
        $this->assertNotSame($defEm, $this->em);

        $reloadedJ = $this->em->find('JMSJobQueueBundle:Job', $j->getId());

        $reloadedWagon = $reloadedJ->findRelatedEntity('JMS\JobQueueBundle\Tests\Functional\TestBundle\Entity\Wagon');
        $reloadedWagon->state = 'broken';
        $defEm->persist($reloadedWagon);
        $defEm->flush();

        $this->assertTrue($defEm->contains($reloadedWagon->train));
    }

    /**
     * Set up data for tests.
     */
    protected function setUp()
    {
        $this->createClient();
        $this->importDatabaseSchema();

        $this->em = self::$kernel->getContainer()->get('doctrine')->getManagerForClass('JMSJobQueueBundle:Job');
        $this->repo = $this->em->getRepository(Job::class);
    }
}
