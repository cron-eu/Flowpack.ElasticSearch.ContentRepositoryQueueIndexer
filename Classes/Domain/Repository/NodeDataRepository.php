<?php
namespace Flowpack\ElasticSearch\ContentRepositoryQueueIndexer\Domain\Repository;

/*
 * This file is part of the Flowpack.ElasticSearch.ContentRepositoryQueueIndexer package.
 *
 * (c) Contributors of the Neos Project - www.neos.io
 *
 * This package is Open Source Software. For the full copyright and license
 * information, please view the LICENSE file which was distributed with this
 * source code.
 */

use Doctrine\Common\Persistence\ObjectManager;
use Doctrine\ORM\Internal\Hydration\IterableResult;
use Doctrine\ORM\QueryBuilder;
use Neos\ContentRepository\Domain\Model\NodeData;
use Neos\Flow\Annotations as Flow;
use Neos\Flow\Persistence\Repository;

/**
 * @Flow\Scope("singleton")
 */
class NodeDataRepository extends \Neos\ContentRepository\Domain\Repository\NodeDataRepository
{

    const ENTITY_CLASSNAME = NodeData::class;

    /**
     * @param string $workspaceName
     * @param integer $firstResult
     * @param integer $maxResults
     * @return IterableResult
     */
    public function findAllBySiteAndWorkspace($workspaceName, $firstResult = 0, $maxResults = 1000)
    {
        /** @var QueryBuilder $queryBuilder */
        $queryBuilder = $this->entityManager->createQueryBuilder();

        $queryBuilder->select('n.Persistence_Object_Identifier persistenceObjectIdentifier, n.identifier identifier, n.dimensionValues dimensions, n.nodeType nodeType, n.path path')
            ->from(NodeData::class, 'n')
            ->where("n.workspace = :workspace AND n.removed = :removed AND n.movedTo IS NULL")
            ->setFirstResult((integer)$firstResult)
            ->setMaxResults((integer)$maxResults)
            ->setParameters([
                ':workspace' => $workspaceName,
                ':removed' => false,
            ]);

        return $queryBuilder->getQuery()->iterate();
    }

}
