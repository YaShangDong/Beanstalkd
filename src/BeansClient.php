<?php declare(strict_types=1);

namespace YaSD\Beanstalkd;

use BadMethodCallException;
use Pheanstalk\Contract\ResponseInterface;
use Pheanstalk\Job as PheanstalkJob;
use Pheanstalk\Pheanstalk;
use Redis;

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
 * @method ?Job              reserveWithTimeout(int $timeout)
 * @method ResponseInterface statsJob(Job $job)
 * @method ResponseInterface statsTube(string $tube)
 * @method ResponseInterface stats()
 * @method void              touch(Job $job)
 * @method $this             useTube(string $tube)
 * @method $this             watch(string $tube)
 * @method $this             watchOnly(string $tube)
 */
class BeansClient
{
    public function __construct(
        protected Redis $redis,
        protected Pheanstalk $pheanstalk,
    ) {
    }

    public function putInTube(string $tube, string $data, int $pri = 1024, int $delay = 0, int $ttr = 60): Job
    {
        $job = $this->pheanstalk->useTube($tube)->put(Job::createJobData($data), $pri, $delay, $ttr);
        return new Job($job, $this);
    }

    public function __call(string $method, array $args): static | mixed
    {
        if (!method_exists($this->pheanstalk, $method)) throw new BadMethodCallException('undefined method: ' . $method);
        $ret = call_user_func_array([$this->pheanstalk, $method], $args);
        if ($ret === $this->pheanstalk) return $this;
        if ($ret instanceof PheanstalkJob) return new Job($ret, $this);
        return $ret;
    }

    public function getPheanstalk(): Pheanstalk
    {
        return $this->pheanstalk;
    }

    public function getRedis(): Redis
    {
        return $this->redis;
    }
}
