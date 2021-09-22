<?php declare(strict_types=1);

namespace YaSD\Beanstalkd\Batch;

use InvalidArgumentException;
use Pheanstalk\Job as PheanstalkJob;
use YaSD\Beanstalkd\Exception\BadJob;
use YaSD\Beanstalkd\Job as BaseJob;

class Job extends BaseJob
{
    protected string $batchId;

    public function __construct(
        PheanstalkJob $job,
        BeansClient $client,
    ) {
        parent::__construct($job, $client);
        $jobArr = $this->decodeJob($job);
        $this->batchId = $jobArr['batchId'];
    }

    protected function decodeJob(PheanstalkJob $job): array
    {
        $json = parent::decodeJob($job);
        if (!array_key_exists('batchId', $json) || !is_string($json['batchId'])) throw BadJob::forInvalidData($job->getData());
        return $json;
    }

    public static function createJobData(string $data, string $batchId = null): string
    {
        if (empty($batchId)) throw new InvalidArgumentException('batchId is empty');

        $created = time();
        return json_encode(compact('created', 'data', 'batchId'));
    }

    public function getBatchId(): string
    {
        return $this->batchId;
    }
}
