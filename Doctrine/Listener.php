<?php

namespace FOS\ElasticaBundle\Doctrine;

use Doctrine\Common\EventArgs;
use Doctrine\Common\Persistence\Event\LifecycleEventArgs;
use Doctrine\Common\Persistence\Event\ManagerEventArgs;
use Doctrine\ODM\MongoDB\PersistentCollection as MongoDBPersistentCollection;
use Doctrine\ORM\Event\PostFlushEventArgs;
use Doctrine\ORM\PersistentCollection as ORMPersistentCollection;
use FOS\ElasticaBundle\Persister\ObjectPersisterInterface;
use FOS\ElasticaBundle\Persister\ObjectPersister;
use FOS\ElasticaBundle\Provider\IndexableInterface;
use Symfony\Component\PropertyAccess\PropertyAccess;
use Symfony\Component\PropertyAccess\PropertyAccessorInterface;

/**
 * Automatically update ElasticSearch based on changes to the Doctrine source
 * data. One listener is generated for each Doctrine entity / ElasticSearch type.
 */
class Listener
{
    /**
     * Object persister
     *
     * @var ObjectPersister
     */
    protected $objectPersister;

    /**
     * Configuration for the listener
     *
     * @var string
     */
    private $config;

    /**
     * Objects scheduled for insertion.
     *
     * @var array
     */
    public $scheduledForInsertion = array();

    /**
     * Objects scheduled to be updated or removed.
     *
     * @var array
     */
    public $scheduledForUpdate = array();

    /**
     * IDs of objects scheduled for removal
     *
     * @var array
     */
    public $scheduledForDeletion = array();

    /**
     * PropertyAccessor instance
     *
     * @var PropertyAccessorInterface
     */
    protected $propertyAccessor;

    /**
     * @var IndexableInterface
     */
    private $indexable;

    /**
     * Constructor.
     *
     * @param ObjectPersisterInterface $objectPersister
     * @param IndexableInterface $indexable
     * @param array $config
     * @param null $logger
     */
    public function __construct(
        ObjectPersisterInterface $objectPersister,
        IndexableInterface $indexable,
        array $config = array(),
        $logger = null
    ) {
        $this->config = array_merge(array(
            'identifier' => 'id',
        ), $config);
        $this->indexable = $indexable;
        $this->objectPersister = $objectPersister;
        $this->propertyAccessor = PropertyAccess::createPropertyAccessor();

        if ($logger) {
            $this->objectPersister->setLogger($logger);
        }
    }

    /**
     * Handles newly created entities that have been persisted to the database
     * The postPersist event must be used so newly persisted entities have their identifier value
     *
     * @param LifecycleEventArgs $eventArgs
     */
    public function postPersist(LifecycleEventArgs $eventArgs)
    {
        $this->scheduleForInsertion($eventArgs->getObject());
    }

    /**
     * Handles updated entities
     *
     * @param LifecycleEventArgs $eventArgs
     */
    public function postUpdate(LifecycleEventArgs $eventArgs)
    {
        $this->scheduleForUpdate($eventArgs->getObject());
    }

    /**
     * Delete objects preRemove instead of postRemove so that we have access to the id.  Because this is called
     * preRemove, first check that the entity is managed by Doctrine
     *
     * @param LifecycleEventArgs $eventArgs
     */
    public function preRemove(LifecycleEventArgs $eventArgs)
    {
        $this->scheduleForDeletion($eventArgs->getObject());
    }

    /**
     * Persist scheduled objects to ElasticSearch
     * After persisting, clear the scheduled queue to prevent multiple data updates when using multiple flush calls
     */
    private function persistScheduled()
    {
        if (count($this->scheduledForInsertion)) {
            $this->objectPersister->insertMany($this->scheduledForInsertion);
            $this->scheduledForInsertion = array();
        }
        if (count($this->scheduledForUpdate)) {
            $this->objectPersister->replaceMany($this->scheduledForUpdate);
            $this->scheduledForUpdate = array();
        }
        if (count($this->scheduledForDeletion)) {
            $this->objectPersister->deleteManyByIdentifiers($this->scheduledForDeletion);
            $this->scheduledForDeletion = array();
        }
    }

    /**
     * Iterate through scheduled actions before flushing to emulate 2.x behavior.
     * Note that the ElasticSearch index will fall out of sync with the source
     * data in the event of a crash during flush.
     *
     * This method is only called in legacy configurations of the listener.
     *
     * @deprecated This method should only be called in applications that depend
     *             on the behaviour that entities are indexed regardless of if a
     *             flush is successful.
     */
    public function preFlush()
    {
        $this->persistScheduled();
    }

    /**
     * Iterating through scheduled actions *after* flushing ensures that the
     * ElasticSearch index will be affected only if the query is successful.
     *
     * @param EventArgs $eventArgs
     */
    public function postFlush(EventArgs $eventArgs)
    {
        $this->scheduleObjectsWithCollectionChanges($eventArgs);
        $this->persistScheduled();
    }

    /**
     * Provides a unified method for scheduling Doctrine objects with collection changes (e.g. ReferenceMany) to be
     * updated in ElasticSearch
     *
     * @param EventArgs $eventArgs
     */
    private function scheduleObjectsWithCollectionChanges(EventArgs $eventArgs)
    {
        $collectionChanges = $this->getCollectionChanges($eventArgs);
        foreach ($collectionChanges as $collection) {
            $this->scheduleForUpdate($collection->getOwner());
        }
    }

    /**
     * Provides a unified method for retrieving a set of collection changes from the Doctrine UnitOfWork
     *
     * @param EventArgs $eventArgs
     * @return array
     * @throws \Exception when encountering an unknown EventArgs instance
     */
    private function getCollectionChanges(EventArgs $eventArgs)
    {
        if ($eventArgs instanceof ManagerEventArgs) {
            $om = $eventArgs->getObjectManager();
        } elseif ($eventArgs instanceof PostFlushEventArgs) {
            $om = $eventArgs->getEntityManager();
        } else {
            throw new \Exception('Unknown EventArgs');
        }

        $uow = $om->getUnitOfWork();

        // Merge updates (adds, removes) and deletes (entire collection removals) and return
        $changes = array_merge($uow->getScheduledCollectionUpdates(), $uow->getScheduledCollectionDeletions());
        return array_filter($changes, function($collection) {
            return $collection instanceof ORMPersistentCollection ||
                $collection instanceof MongoDBPersistentCollection;
        });
    }

    /**
     * Schedules a Doctrine object (entity/document) to be updated in Elasticsearch
     *
     * @param mixed $object
     */
    private function scheduleForUpdate($object)
    {
        if (!$this->objectPersister->handlesObject($object)) {
            return;
        }

        if ($this->isObjectIndexable($object)) {
            $this->scheduledForUpdate[spl_object_hash($object)] = $object;
        } else {
            // Delete if no longer indexable
            $this->scheduleForDeletion($object);
        }
    }

    /**
     * Schedules a Doctrine object (entity/document) for insertion into Elasticsearch
     *
     * @param mixed $object
     */
    private function scheduleForInsertion($object)
    {
        if (!$this->objectPersister->handlesObject($object) || !$this->isObjectIndexable($object)) {
            return;
        }

        $this->scheduledForInsertion[spl_object_hash($object)] = $object;
    }

    /**
     * Record the specified identifier to delete. Do not need to entire object.
     *
     * @param object $object
     */
    private function scheduleForDeletion($object)
    {
        if (!$this->objectPersister->handlesObject($object)) {
            return;
        }

        $identifierValue = $this->propertyAccessor->getValue($object, $this->config['identifier']);
        if (!$identifierValue) {
            return;
        }

        $this->scheduledForDeletion[spl_object_hash($object)] = $identifierValue;
    }

    /**
     * Checks if the object is indexable or not.
     *
     * @param object $object
     * @return bool
     */
    private function isObjectIndexable($object)
    {
        return $this->indexable->isObjectIndexable(
            $this->config['indexName'],
            $this->config['typeName'],
            $object
        );
    }
}
