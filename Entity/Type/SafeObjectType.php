<?php

namespace JMS\JobQueueBundle\Entity\Type;

use Doctrine\DBAL\Types\ObjectType;

class SafeObjectType extends ObjectType
{
    /**
     * @param array                                     $fieldDeclaration
     * @param \Doctrine\DBAL\Platforms\AbstractPlatform $platform
     *
     * @return string
     */
    public function getSQLDeclaration(array $fieldDeclaration, \Doctrine\DBAL\Platforms\AbstractPlatform $platform)
    {
        return $platform->getBlobTypeDeclarationSQL($fieldDeclaration);
    }

    /**
     * @return string
     */
    public function getName()
    {
        return 'jms_job_safe_object';
    }
}
