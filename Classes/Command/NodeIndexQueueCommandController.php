<?php
namespace Flowpack\ElasticSearch\ContentRepositoryQueueIndexer\Command;

/*
 * This file is part of the Flowpack.ElasticSearch.ContentRepositoryQueueIndexer package.
 *
 * (c) Contributors of the Neos Project - www.neos.io
 *
 * This package is Open Source Software. For the full copyright and license
 * information, please view the LICENSE file which was distributed with this
 * source code.
 */

use Flowpack\ElasticSearch\ContentRepositoryAdaptor\Driver\NodeTypeMappingBuilderInterface;
use Flowpack\ElasticSearch\ContentRepositoryAdaptor\Indexer\NodeIndexer;
use Flowpack\ElasticSearch\ContentRepositoryQueueIndexer\Domain\Repository\NodeDataRepository;
use Flowpack\ElasticSearch\ContentRepositoryQueueIndexer\IndexingJob;
use Flowpack\ElasticSearch\ContentRepositoryQueueIndexer\LoggerTrait;
use Flowpack\ElasticSearch\ContentRepositoryQueueIndexer\UpdateAliasJob;
use Flowpack\ElasticSearch\Domain\Model\Mapping;
use Flowpack\JobQueue\Common\Exception;
use Flowpack\JobQueue\Common\Job\JobManager;
use Flowpack\JobQueue\Common\Queue\QueueManager;
use Neos\ContentRepository\Domain\Repository\WorkspaceRepository;
use Neos\Flow\Annotations as Flow;
use Neos\Flow\Cli\CommandController;
use Neos\Flow\Persistence\PersistenceManagerInterface;
use Neos\Utility\Files;

/**
 * Provides CLI features for index handling
 *
 * @Flow\Scope("singleton")
 */
class NodeIndexQueueCommandController extends CommandController
{
    use LoggerTrait;

    const BATCH_QUEUE_NAME = 'Flowpack.ElasticSearch.ContentRepositoryQueueIndexer';
    const LIVE_QUEUE_NAME = 'Flowpack.ElasticSearch.ContentRepositoryQueueIndexer.Live';

    /**
     * @var JobManager
     * @Flow\Inject
     */
    protected $jobManager;

    /**
     * @var QueueManager
     * @Flow\Inject
     */
    protected $queueManager;

    /**
     * @var PersistenceManagerInterface
     * @Flow\Inject
     */
    protected $persistenceManager;

    /**
     * @var NodeTypeMappingBuilderInterface
     * @Flow\Inject
     */
    protected $nodeTypeMappingBuilder;

    /**
     * @var NodeDataRepository
     * @Flow\Inject
     */
    protected $nodeDataRepository;

    /**
     * @var WorkspaceRepository
     * @Flow\Inject
     */
    protected $workspaceRepository;

    /**
     * @var NodeIndexer
     * @Flow\Inject
     */
    protected $nodeIndexer;

    /**
     * @Flow\InjectConfiguration(path="batchSize")
     * @var int
     */
    protected $batchSize;

    /**
     * Index all nodes by creating a new index and when everything was completed, switch the index alias.
     *
     * @param string $workspace
     * @throws \Flowpack\JobQueue\Common\Exception
     * @throws \Neos\Flow\Mvc\Exception\StopActionException
     * @throws \Flowpack\ElasticSearch\ContentRepositoryAdaptor\Exception
     */
    public function buildCommand($workspace = null)
    {
        $indexPostfix = time();
        $indexName = $this->createNextIndex($indexPostfix);
        $this->updateMapping($indexPostfix);

        $this->outputLine();
        $this->outputLine('<b>Indexing on %s ...</b>', [$indexName]);

        $pendingJobs = $this->queueManager->getQueue(self::BATCH_QUEUE_NAME)->countReady();
        if ($pendingJobs !== 0) {
            $this->outputLine('<error>!! </error> The queue "%s" is not empty (%d pending jobs), please flush the queue.', [self::BATCH_QUEUE_NAME, $pendingJobs]);
            $this->quit(1);
        }

        if ($workspace === null) {
            foreach ($this->workspaceRepository->findAll() as $workspace) {
                $workspace = $workspace->getName();
                $this->outputLine();
                $this->indexWorkspace($workspace, $indexPostfix);
            }
        } else {
            $this->outputLine();
            $this->indexWorkspace($workspace, $indexPostfix);
        }
        $updateAliasJob = new UpdateAliasJob($indexPostfix);
        $this->jobManager->queue(self::BATCH_QUEUE_NAME, $updateAliasJob);

        $this->outputLine("Indexing jobs created for queue %s with success ...", [self::BATCH_QUEUE_NAME]);
        $this->outputSystemReport();
        $this->outputLine();
    }

    /**
     * Flush the index queue
     */
    public function flushCommand()
    {
        try {
            $this->queueManager->getQueue(self::BATCH_QUEUE_NAME)->flush();
            $this->outputSystemReport();
        } catch (Exception $exception) {
            $this->outputLine('An error occurred: %s', [$exception->getMessage()]);
        }
        $this->outputLine();
    }

    /**
     * Output system report for CLI commands
     */
    protected function outputSystemReport()
    {
        $this->outputLine();
        $this->outputLine('Memory Usage   : %s', [Files::bytesToSizeString(memory_get_peak_usage(true))]);
        $time = microtime(true) - $_SERVER["REQUEST_TIME_FLOAT"];
        $this->outputLine('Execution time : %s seconds', [$time]);
        $this->outputLine('Indexing Queue : %s', [self::BATCH_QUEUE_NAME]);
        try {
            $queue = $this->queueManager->getQueue(self::BATCH_QUEUE_NAME);
            $this->outputLine('Pending Jobs   : %s', [$queue->countReady()]);
            $this->outputLine('Reserved Jobs  : %s', [$queue->countReserved()]);
            $this->outputLine('Failed Jobs    : %s', [$queue->countFailed()]);
        } catch (Exception $exception) {
            $this->outputLine('Pending Jobs   : Error, queue %s not found, %s', [self::BATCH_QUEUE_NAME, $exception->getMessage()]);
        }
    }

    /**
     * @param string $workspaceName
     * @param string $indexPostfix
     */
    protected function indexWorkspace($workspaceName, $indexPostfix)
    {
        $this->outputLine('<info>++</info> Indexing %s workspace..', [$workspaceName]);
        $this->outputLine();

        $total = $this->nodeDataRepository->countBySiteAndWorkspace($workspaceName);
        $this->output->progressStart($total);

        $nodeCounter = 0;
        $iterator = $this->nodeDataRepository->findAllBySiteAndWorkspace($workspaceName);

        $jobData = [];

        $createBatch = function() use ($nodeCounter, $indexPostfix, $workspaceName, &$jobData) {
            $indexingJob = new IndexingJob($indexPostfix, $workspaceName, $jobData);
            $this->jobManager->queue(self::BATCH_QUEUE_NAME, $indexingJob);
            $this->persistenceManager->clearState();
            $this->output->progressAdvance(count($jobData));
            $jobData = [];
        };

        foreach ($this->nodeDataRepository->iterate($iterator) as $data) {
            $jobData[] = [
                'persistenceObjectIdentifier' => $data['persistenceObjectIdentifier'],
                'identifier' => $data['identifier'],
                'dimensions' => $data['dimensions'],
                'workspace' => $workspaceName,
                'nodeType' => $data['nodeType'],
                'path' => $data['path'],
            ];
            $nodeCounter++;
            if ($nodeCounter % $this->batchSize === 0) {
                $createBatch();
            }
        }

        if ($jobData) {
            $createBatch();
        }

        $this->output->progressFinish();
        $this->outputLine();
        $this->outputLine();
    }

    /**
     * @param string $indexPostfix
     * @return string
     * @throws \Flowpack\ElasticSearch\ContentRepositoryAdaptor\Exception
     */
    protected function createNextIndex($indexPostfix)
    {
        $this->nodeIndexer->setIndexNamePostfix($indexPostfix);
        $this->nodeIndexer->getIndex()->create();
        $this->log(sprintf('action=indexing step=index-created index=%s', $this->nodeIndexer->getIndexName()), LOG_INFO);

        return $this->nodeIndexer->getIndexName();
    }

    /**
     * Update Index Mapping
     *
     * @return void
     * @throws \Flowpack\ElasticSearch\ContentRepositoryAdaptor\Exception
     */
    protected function updateMapping($indexPostfix)
    {
        $nodeTypeMappingCollection = $this->nodeTypeMappingBuilder->buildMappingInformation($this->nodeIndexer->getIndex());
        foreach ($nodeTypeMappingCollection as $mapping) {
            $this->nodeIndexer->setIndexNamePostfix($indexPostfix);
            /** @var Mapping $mapping */
            $mapping->apply();
        }
        $this->log(sprintf('action=indexing step=mapping-updated index=%s', $this->nodeIndexer->getIndexName()), LOG_INFO);
    }
}
