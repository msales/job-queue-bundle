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

namespace JMS\JobQueueBundle\Entity;

use Doctrine\Common\Collections\ArrayCollection;
use Doctrine\ORM\Mapping as ORM;
use JMS\JobQueueBundle\Exception\InvalidStateTransitionException;
use JMS\JobQueueBundle\Exception\LogicException;
use Symfony\Component\Debug\Exception\FlattenException;

/**
 * @ORM\Entity(repositoryClass = "JMS\JobQueueBundle\Entity\Repository\JobRepository")
 * @ORM\Table(name = "jms_jobs", indexes = {
 *     @ORM\Index("cmd_search_index", columns = {"command", "executeAfter"}),
 *     @ORM\Index("sorting_index", columns = {"state", "priority", "id"}),
 *     @ORM\Index("count_state_index", columns = {"state", "queue"}),
 *     @ORM\Index("removal_fetch_index", columns= {"executeAfter", "originalJob_id"}),
 * })
 * @ORM\ChangeTrackingPolicy("DEFERRED_EXPLICIT")
 *
 * @author Johannes M. Schmitt <schmittjoh@gmail.com>
 */
class Job
{
    /** State if job is inserted, but not yet ready to be started. */
    const STATE_NEW = 'new';

    /**
     * State if job is inserted, and might be started.
     *
     * It is important to note that this does not automatically mean that all
     * jobs of this state can actually be started, but you have to check
     * isStartable() to be absolutely sure.
     *
     * In contrast to NEW, jobs of this state at least might be started,
     * while jobs of state NEW never are allowed to be started.
     */
    const STATE_PENDING = 'pending';

    /** State if job was never started, and will never be started. */
    const STATE_CANCELED = 'canceled';

    /** State if job was started and has not exited, yet. */
    const STATE_RUNNING = 'running';

    /** State if job exists with a successful exit code. */
    const STATE_FINISHED = 'finished';

    /** State if job exits with a non-successful exit code. */
    const STATE_FAILED = 'failed';

    /** State if job exceeds its configured maximum runtime. */
    const STATE_TERMINATED = 'terminated';

    /**
     * State if an error occurs in the runner command.
     *
     * The runner command is the command that actually launches the individual
     * jobs. If instead an error occurs in the job command, this will result
     * in a state of FAILED.
     */
    const STATE_INCOMPLETE = 'incomplete';

    /**
     * State if an error occurs in the runner command.
     *
     * The runner command is the command that actually launches the individual
     * jobs. If instead an error occurs in the job command, this will result
     * in a state of FAILED.
     */
    const DEFAULT_QUEUE = 'default';
    const MAX_QUEUE_LENGTH = 50;

    const PRIORITY_LOW = -5;
    const PRIORITY_DEFAULT = 0;
    const PRIORITY_HIGH = 5;

    /** @ORM\Id @ORM\GeneratedValue(strategy = "AUTO") @ORM\Column(type = "bigint", options = {"unsigned": true}) */
    private $id;

    /** @ORM\Column(type = "string", length = 15) */
    private $state;

    /** @ORM\Column(type = "string", length = Job::MAX_QUEUE_LENGTH) */
    private $queue;

    /** @ORM\Column(type = "smallint") */
    private $priority = 0;

    /** @ORM\Column(type = "datetime", name="createdAt") */
    private $createdAt;

    /** @ORM\Column(type = "datetime", name="startedAt", nullable = true) */
    private $startedAt;

    /** @ORM\Column(type = "datetime", name="checkedAt", nullable = true) */
    private $checkedAt;

    /** @ORM\Column(type = "datetime", name="executeAfter", nullable = true) */
    private $executeAfter;

    /** @ORM\Column(type = "datetime", name="closedAt", nullable = true) */
    private $closedAt;

    /** @ORM\Column(type = "text") */
    private $command;

    /** @ORM\Column(type = "json_array") */
    private $args;

    /**
     * @ORM\ManyToMany(targetEntity = "Job", fetch = "EAGER")
     * @ORM\JoinTable(name="jms_job_dependencies",
     *     joinColumns = { @ORM\JoinColumn(name = "source_job_id", referencedColumnName = "id") },
     *     inverseJoinColumns = { @ORM\JoinColumn(name = "dest_job_id", referencedColumnName = "id")}
     * )
     */
    private $dependencies;

    /** @ORM\Column(type = "text", nullable = true) */
    private $output;

    /** @ORM\Column(type = "text", name="errorOutput", nullable = true) */
    private $errorOutput;

    /** @ORM\Column(type = "smallint", name="exitCode", nullable = true, options = {"unsigned": true}) */
    private $exitCode;

    /** @ORM\Column(type = "smallint", name="maxRuntime", options = {"unsigned": true}) */
    private $maxRuntime = 0;

    /** @ORM\Column(type = "smallint", name="maxRetries", options = {"unsigned": true}) */
    private $maxRetries = 0;

    /**
     * @ORM\ManyToOne(targetEntity = "Job", inversedBy = "retryJobs")
     * @ORM\JoinColumn(name="originalJob_id", referencedColumnName="id")
     */
    private $originalJob;

    /** @ORM\OneToMany(targetEntity = "Job", mappedBy = "originalJob", cascade = {"persist", "remove", "detach"}) */
    private $retryJobs;

    /** @ORM\Column(type = "jms_job_safe_object", name="stackTrace", nullable = true) */
    private $stackTrace;

    /** @ORM\Column(type = "smallint", nullable = true, options = {"unsigned": true}) */
    private $runtime;

    /** @ORM\Column(type = "integer", name="memoryUsage", nullable = true, options = {"unsigned": true}) */
    private $memoryUsage;

    /** @ORM\Column(type = "integer", name="memoryUsageReal", nullable = true, options = {"unsigned": true}) */
    private $memoryUsageReal;

    /**
     * This may store any entities which are related to this job, and are
     * managed by Doctrine.
     *
     * It is effectively a many-to-any association.
     */
    private $relatedEntities;

    /**
     * Job constructor.
     *
     * @param        $command
     * @param array  $args
     * @param bool   $confirmed
     * @param string $queue
     * @param int    $priority
     */
    public function __construct($command, array $args = array(), $confirmed = true, $queue = self::DEFAULT_QUEUE, $priority = self::PRIORITY_DEFAULT)
    {
        if (trim($queue) === '') {
            throw new \InvalidArgumentException('$queue must not be empty.');
        }
        if (strlen($queue) > self::MAX_QUEUE_LENGTH) {
            throw new \InvalidArgumentException(sprintf('The maximum queue length is %d, but got "%s" (%d chars).', self::MAX_QUEUE_LENGTH, $queue, strlen($queue)));
        }

        $this->command = $command;
        $this->args = $args;
        $this->state = $confirmed ? self::STATE_PENDING : self::STATE_NEW;
        $this->queue = $queue;
        $this->priority = $priority * -1;
        $this->createdAt = new \DateTime();
        $this->executeAfter = new \DateTime();
        $this->executeAfter = $this->executeAfter->modify('-1 second');
        $this->dependencies = new ArrayCollection();
        $this->retryJobs = new ArrayCollection();
        $this->relatedEntities = new ArrayCollection();
    }

    /**
     * Job clone.
     */
    public function __clone()
    {
        $this->state = self::STATE_PENDING;
        $this->createdAt = new \DateTime();
        $this->startedAt = null;
        $this->checkedAt = null;
        $this->closedAt = null;
        $this->output = null;
        $this->errorOutput = null;
        $this->exitCode = null;
        $this->stackTrace = null;
        $this->runtime = null;
        $this->memoryUsage = null;
        $this->memoryUsageReal = null;
        $this->relatedEntities = new ArrayCollection();
    }

    /**
     * @return mixed
     */
    public function getId()
    {
        return $this->id;
    }

    /**
     * @return string
     */
    public function getState()
    {
        return $this->state;
    }

    /**
     * @return float|int
     */
    public function getPriority()
    {
        return $this->priority * -1;
    }

    /**
     * @return bool
     */
    public function isStartable()
    {
        foreach ($this->dependencies as $dep) {
            if ($dep->getState() !== self::STATE_FINISHED) {
                return false;
            }
        }

        return true;
    }

    /**
     * @param $newState
     */
    public function setState($newState)
    {
        if ($newState === $this->state) {
            return;
        }

        switch ($this->state) {
            case self::STATE_NEW:
                if ( ! in_array($newState, array(self::STATE_PENDING, self::STATE_CANCELED), true)) {
                    throw new InvalidStateTransitionException($this, $newState, array(self::STATE_PENDING, self::STATE_CANCELED));
                }

                if (self::STATE_CANCELED === $newState) {
                    $this->closedAt = new \DateTime();
                }

                break;

            case self::STATE_PENDING:
                if ( ! in_array($newState, array(self::STATE_RUNNING, self::STATE_CANCELED), true)) {
                    throw new InvalidStateTransitionException($this, $newState, array(self::STATE_RUNNING, self::STATE_CANCELED));
                }

                if ($newState === self::STATE_RUNNING) {
                    $this->startedAt = new \DateTime();
                    $this->checkedAt = new \DateTime();
                } else if ($newState === self::STATE_CANCELED) {
                    $this->closedAt = new \DateTime();
                }

                break;

            case self::STATE_RUNNING:
                if ( ! in_array($newState, array(self::STATE_FINISHED, self::STATE_FAILED, self::STATE_TERMINATED, self::STATE_INCOMPLETE))) {
                    throw new InvalidStateTransitionException($this, $newState, array(self::STATE_FINISHED, self::STATE_FAILED, self::STATE_TERMINATED, self::STATE_INCOMPLETE));
                }

                $this->closedAt = new \DateTime();

                break;

            case self::STATE_FINISHED:
            case self::STATE_FAILED:
            case self::STATE_TERMINATED:
            case self::STATE_INCOMPLETE:
                throw new InvalidStateTransitionException($this, $newState);

            default:
                throw new LogicException('The previous cases were exhaustive. Unknown state: '.$this->state);
        }

        $this->state = $newState;
    }

    /**
     * @return \DateTime
     */
    public function getCreatedAt()
    {
        return $this->createdAt;
    }

    /**
     * @return mixed
     */
    public function getClosedAt()
    {
        return $this->closedAt;
    }

    /**
     * @return static
     */
    public function getExecuteAfter()
    {
        return $this->executeAfter;
    }

    /**
     * @param \DateTime $executeAfter
     */
    public function setExecuteAfter(\DateTime $executeAfter)
    {
        $this->executeAfter = $executeAfter;
    }

    /**
     * @return mixed
     */
    public function getCommand()
    {
        return $this->command;
    }

    /**
     * @return array
     */
    public function getArgs()
    {
        return $this->args;
    }

    /**
     * @return ArrayCollection
     */
    public function getRelatedEntities()
    {
        return $this->relatedEntities;
    }

    /**
     * @return bool
     */
    public function isClosedNonSuccessful()
    {
        return in_array($this->state, array(self::STATE_CANCELED, self::STATE_FAILED, self::STATE_INCOMPLETE, self::STATE_TERMINATED), true);
    }

    /**
     * @param $class
     *
     * @return mixed|null
     */
    public function findRelatedEntity($class)
    {
        foreach ($this->relatedEntities as $entity) {
            if ($entity instanceof $class) {
                return $entity;
            }
        }

        return null;
    }

    /**
     * @param $entity
     */
    public function addRelatedEntity($entity)
    {
        assert('is_object($entity)');

        if ($this->relatedEntities->contains($entity)) {
            return;
        }

        $this->relatedEntities->add($entity);
    }

    /**
     * @return ArrayCollection
     */
    public function getDependencies()
    {
        return $this->dependencies;
    }

    /**
     * @param Job $job
     *
     * @return bool
     */
    public function hasDependency(Job $job)
    {
        return $this->dependencies->contains($job);
    }

    /**
     * @param Job $job
     */
    public function addDependency(Job $job)
    {
        if ($this->dependencies->contains($job)) {
            return;
        }

        if ($this->mightHaveStarted()) {
            throw new \LogicException('You cannot add dependencies to a job which might have been started already.');
        }

        $this->dependencies->add($job);
    }

    /**
     * @return mixed
     */
    public function getRuntime()
    {
        return $this->runtime;
    }

    /**
     * @param $time
     */
    public function setRuntime($time)
    {
        $this->runtime = (integer) $time;
    }

    /**
     * @return mixed
     */
    public function getMemoryUsage()
    {
        return $this->memoryUsage;
    }

    /**
     * @return mixed
     */
    public function getMemoryUsageReal()
    {
        return $this->memoryUsageReal;
    }

    /**
     * @param $output
     */
    public function addOutput($output)
    {
        $this->output .= $output;
    }

    /**
     * @param $output
     */
    public function addErrorOutput($output)
    {
        $this->errorOutput .= $output;
    }

    /**
     * @param $output
     */
    public function setOutput($output)
    {
        $this->output = $output;
    }

    /**
     * @param $output
     */
    public function setErrorOutput($output)
    {
        $this->errorOutput = $output;
    }

    /**
     * @return mixed
     */
    public function getOutput()
    {
        return $this->output;
    }

    /**
     * @return mixed
     */
    public function getErrorOutput()
    {
        return $this->errorOutput;
    }

    /**
     * @param $code
     */
    public function setExitCode($code)
    {
        $this->exitCode = $code;
    }

    /**
     * @return mixed
     */
    public function getExitCode()
    {
        return $this->exitCode;
    }

    /**
     * @param $time
     */
    public function setMaxRuntime($time)
    {
        $this->maxRuntime = (integer) $time;
    }

    /**
     * @return int
     */
    public function getMaxRuntime()
    {
        return $this->maxRuntime;
    }

    /**
     * @return mixed
     */
    public function getStartedAt()
    {
        return $this->startedAt;
    }

    /**
     * @return int
     */
    public function getMaxRetries()
    {
        return $this->maxRetries;
    }

    /**
     * @param $tries
     */
    public function setMaxRetries($tries)
    {
        $this->maxRetries = (integer) $tries;
    }

    /**
     * @return bool
     */
    public function isRetryAllowed()
    {
        // If no retries are allowed, we can bail out directly, and we
        // do not need to initialize the retryJobs relation.
        if (0 === $this->maxRetries) {
            return false;
        }

        return count($this->retryJobs) < $this->maxRetries;
    }

    /**
     * @return $this
     */
    public function getOriginalJob()
    {
        if (null === $this->originalJob) {
            return $this;
        }

        return $this->originalJob;
    }

    /**
     * @param Job $job
     */
    public function setOriginalJob(Job $job)
    {
        if (self::STATE_PENDING !== $this->state) {
            throw new \LogicException($this.' must be in state "PENDING".');
        }

        if (null !== $this->originalJob) {
            throw new \LogicException($this.' already has an original job set.');
        }

        $this->originalJob = $job;
    }

    /**
     * @param Job $job
     */
    public function addRetryJob(Job $job)
    {
        if (self::STATE_RUNNING !== $this->state) {
            throw new \LogicException('Retry jobs can only be added to running jobs.');
        }

        $job->setOriginalJob($this);
        $this->retryJobs->add($job);
    }

    /**
     * @return ArrayCollection
     */
    public function getRetryJobs()
    {
        return $this->retryJobs;
    }

    /**
     * @return bool
     */
    public function isRetryJob()
    {
        return null !== $this->originalJob;
    }

    /**
     * Mark checkedAt for current time when called.
     */
    public function checked()
    {
        $this->checkedAt = new \DateTime();
    }

    /**
     * @return mixed
     */
    public function getCheckedAt()
    {
        return $this->checkedAt;
    }

    /**
     * @param FlattenException $ex
     */
    public function setStackTrace(FlattenException $ex)
    {
        $this->stackTrace = $ex;
    }

    /**
     * @return mixed
     */
    public function getStackTrace()
    {
        return $this->stackTrace;
    }

    /**
     * @return string
     */
    public function getQueue()
    {
        return $this->queue;
    }

    /**
     * @return bool
     */
    public function isNew()
    {
        return self::STATE_NEW === $this->state;
    }

    /**
     * @return bool
     */
    public function isPending()
    {
        return self::STATE_PENDING === $this->state;
    }

    /**
     * @return bool
     */
    public function isCanceled()
    {
        return self::STATE_CANCELED === $this->state;
    }

    /**
     * @return bool
     */
    public function isRunning()
    {
        return self::STATE_RUNNING === $this->state;
    }

    /**
     * @return bool
     */
    public function isTerminated()
    {
        return self::STATE_TERMINATED === $this->state;
    }

    /**
     * @return bool
     */
    public function isFailed()
    {
        return self::STATE_FAILED === $this->state;
    }

    /**
     * @return bool
     */
    public function isFinished()
    {
        return self::STATE_FINISHED === $this->state;
    }

    /**
     * @return bool
     */
    public function isIncomplete()
    {
        return self::STATE_INCOMPLETE === $this->state;
    }

    /**
     * @return string
     */
    public function __toString()
    {
        return sprintf('Job(id = %s, command = "%s")', $this->id, $this->command);
    }

    /**
     * @return bool
     */
    private function mightHaveStarted()
    {
        if (null === $this->id) {
            return false;
        }

        if (self::STATE_NEW === $this->state) {
            return false;
        }

        if (self::STATE_PENDING === $this->state && ! $this->isStartable()) {
            return false;
        }

        return true;
    }
}
