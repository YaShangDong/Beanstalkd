<?php declare(strict_types=1);

namespace YaSD\Beanstalkd;

use BadMethodCallException;
use Pheanstalk\Job as PheanstalkJob;
use Pheanstalk\Pheanstalk;
use Redis;

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
