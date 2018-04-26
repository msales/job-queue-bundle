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

namespace JMS\JobQueueBundle\DependencyInjection;

use JMS\JobQueueBundle\Command\CleanUpCommand;
use JMS\JobQueueBundle\Command\MarkJobIncompleteCommand;
use JMS\JobQueueBundle\Command\RunCommand;
use JMS\JobQueueBundle\Entity\Job;
use JMS\JobQueueBundle\Entity\Repository\JobManager;
use JMS\JobQueueBundle\Entity\Repository\JobRepository;
use Symfony\Component\DependencyInjection\ContainerBuilder;
use Symfony\Component\Config\FileLocator;
use Symfony\Component\DependencyInjection\Reference;
use Symfony\Component\HttpKernel\DependencyInjection\Extension;
use Symfony\Component\DependencyInjection\Loader;

/**
 * This is the class that loads and manages your bundle configuration
 *
 * To learn more see {@link http://symfony.com/doc/current/cookbook/bundles/extension.html}
 */
class JMSJobQueueExtension extends Extension
{
    /**
     * {@inheritDoc}
     */
    public function load(array $configs, ContainerBuilder $container)
    {
        $configuration = new Configuration();
        $config = $this->processConfiguration($configuration, $configs);

        $loader = new Loader\XmlFileLoader($container, new FileLocator(__DIR__ . '/../Resources/config'));
        $loader->load('services.xml');

        $container->setParameter('jms_job_queue.statistics', $config['statistics']);
        if ($config['statistics']) {
            $loader->load('statistics.xml');
        }

        $container->setParameter('jms_job_queue.queue_options_defaults', $config['queue_options_defaults']);
        $container->setParameter('jms_job_queue.queue_options', $config['queue_options']);

        $container->register('doctrine.orm.job_entity_repository', JobRepository::class)
            ->setFactory([new Reference("doctrine.orm.entity_manager"), 'getRepository'])
            ->addArgument('JMSJobQueueBundle:Job')
        ;

        $container->register('doctrine.orm.job_manager', JobManager::class)
            ->addArgument(new Reference('doctrine.orm.job_entity_repository'))
            ->addArgument(new Reference('doctrine.orm.entity_manager'))
            ->addArgument(new Reference('event_dispatcher'))
            ->setPublic(true)
        ;

        $container->register(RunCommand::class)
            ->addArgument(new Reference('doctrine.orm.job_manager'))
            ->addArgument($container->getParameter('jms_job_queue.queue_options_defaults'))
            ->addArgument($container->getParameter('jms_job_queue.queue_options'))
            ->addArgument($container->getParameter('kernel.root_dir'))
            ->addTag('console.command', ['command' => 'jms-job-queue:run'])
        ;

        $container->register(MarkJobIncompleteCommand::class)
            ->addArgument(new Reference('doctrine.orm.job_manager'))
            ->addTag('console.command', ['command' => 'jms-job-queue:mark-incomplete'])
        ;

        $container->register(CleanUpCommand::class)
            ->addArgument(new Reference('doctrine.orm.entity_manager'))
            ->addTag('console.command', ['command' => 'jms-job-queue:clean-up'])
        ;
    }
}
