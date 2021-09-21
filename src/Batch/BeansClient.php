<?php declare(strict_types=1);

namespace YaSD\Beanstalkd\Batch;

use InvalidArgumentException;
use Pheanstalk\Contract\ResponseInterface;
use YaSD\Beanstalkd\BeansClient as BaseBeansClient;

/**
 * @method void              bury(Job $job, int $priority = 1024)
 * @method void              delete(Job $job)
 * @method $this             ignore(string $tube)
 * @method int               kick(int $max)
 * @method void              kickJob(Job $job)
 * @method string[]          listTubes()
 * @method string[]          listTubesWatched(bool $askServer = false)
 * @method string            listTubeUsed(bool $askServer = false)
 * @method void              pauseTube(string $tube, int $delay)
 * @method void              resumeTube(string $tube)
 * @method Job               peek(Job $job)
 * @method ?Job              peekReady()
 * @method ?Job              peekDelayed()
 * @method ?Job              peekBuried()
 * @method Job               put(string $data, int $priority = 1024, int $delay = 0, int $ttr = 60)
 * @method void              release(Job $job, int $priority = 1024, int $delay = 0)
 * @method ?Job              reserve()
 * @method ResponseInterface statsJob(Job $job)
 * @method ResponseInterface statsTube(string $tube)
 * @method ResponseInterface stats()
 * @method void              touch(Job $job)
 * @method $this             useTube(string $tube)
 * @method $this             watch(string $tube)
 * @method $this             watchOnly(string $tube)
 */
class BeansClient extends BaseBeansClient
{
    public static string $redisKeyForBatchTube = 'Runtime:BatchTube:%s';

    public function putInTube(string $tube, string $data, int $pri = 1024, int $delay = 0, int $ttr = 60, string $batchId = null): Job
    {
        if (empty($batchId)) throw new InvalidArgumentException('batchId is empty');

        $redisKey = sprintf(static::$redisKeyForBatchTube, $batchId);
        $this->redis->set($redisKey, $tube);

        $job = $this->pheanstalk->useTube($tube)->put(Job::createJobData($data, $batchId), $pri, $delay, $ttr);
        return new Job($job, $this);
    }

    public function reserveWithTimeout(int $timeout): ?Job
    {
        $start = microtime(true);
        $ret = $this->pheanstalk->reserveWithTimeout($timeout);
        if ($ret) {
            $job = new Job($ret, $this);
            if ($job->getTube() === $this->getBatchTube($job->getBatchId())) return $job;
            $job->done();
            $leftTime = $timeout - (int) (microtime(true) - $start);
            if ($leftTime > 0) {
                return $this->reserveWithTimeout($leftTime);
            }
        }
        return null;
    }

    private function getBatchTube(string $batchId): ?string
    {
        $redisKey = sprintf(static::$redisKeyForBatchTube, $batchId);
        $tube = $this->redis->get($redisKey);
        if (false === $tube) return null; // key not exist
        return (string) $tube;
    }
}
