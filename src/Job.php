<?php declare(strict_types=1);

namespace YaSD\Beanstalkd;

use JsonException;
use Pheanstalk\Contract\JobIdInterface;
use Pheanstalk\Contract\ResponseInterface;
use Pheanstalk\Job as PheanstalkJob;
use YaSD\Beanstalkd\Exception\BadJob;

class Job implements JobIdInterface
{
    public static string $redisKeyForJobReleases = 'Runtime:JobReleases:%s';

    protected int $id;
    protected string $uid;
    protected string $data;
    protected int $created;

    public function __construct(
        PheanstalkJob $job,
        protected BeansClient $client,
    ) {
        $this->id = $job->getId();
        $jobArr = $this->decodeJob($job);
        $this->data = $jobArr['data'];
        $this->created = $jobArr['created'];
        $this->uid = sha1(sprintf('%d.%d.%s', $this->id, $this->created, $this->data));
    }

    protected function decodeJob(PheanstalkJob $job): array
    {
        $ret = $job->getData();

        try {
            $json = json_decode($ret, true, flags: JSON_THROW_ON_ERROR);
        } catch (JsonException $e) {
            throw BadJob::forInvalidData($ret, $e);
        }

        if (!array_key_exists('data', $json) || !is_string($json['data'])) throw BadJob::forInvalidData($ret);
        if (!array_key_exists('created', $json) || !is_int($json['created'])) throw BadJob::forInvalidData($ret);

        return $json;
    }

    public static function createJobData(string $data): string
    {
        $created = time();
        return json_encode(compact('created', 'data'));
    }

    public function getId(): int
    {
        return $this->id;
    }

    public function getData(): string
    {
        return $this->data;
    }

    public function getJsonData(): array
    {
        try {
            return json_decode($this->getData(), true, flags: JSON_THROW_ON_ERROR);
        } catch (JsonException $e) {
            throw BadJob::forInvalidData($this->getData(), $e);
        }
    }

    public function getReleases(): int
    {
        return (int) $this->client->getRedis()->get($this->getRedisKeyForReleases());
    }

    public function getTube(): string
    {
        return (string) $this->getStats()['tube'];
    }

    public function getPri(): int
    {
        return (int) $this->getStats()['pri'];
    }

    public function done(): void
    {
        $this->client->getRedis()->del($this->getRedisKeyForReleases());
        $this->client->getPheanstalk()->delete($this);
    }

    public function bury(): void
    {
        $this->client->getRedis()->del($this->getRedisKeyForReleases());
        $pri = $this->getPri();
        $this->client->getPheanstalk()->bury($this, $pri);
    }

    /**
     * @return true  release
     * @return false bury
     */
    public function releaseOrBury(?int $maxReleases = null, int $releaseDelay = 0, int $priorityChange = 0): bool
    {
        $releases = $this->incrReleaseCounting();
        if ($maxReleases && $releases >= $maxReleases) {
            $this->bury();
            return false; // not release, bury
        }
        $this->releaseWithoutCounting($releaseDelay, $priorityChange);
        return true;
    }

    public function release(int $releaseDelay = 0, int $priorityChange = 0): static
    {
        $this->incrReleaseCounting();
        $this->releaseWithoutCounting($releaseDelay, $priorityChange);
        return $this;
    }

    public function releaseWithoutCounting(int $releaseDelay = 0, int $priorityChange = 0): static
    {
        $pri = max(0, $this->getPri() + $priorityChange);
        $this->client->getPheanstalk()->release($this, $pri, $releaseDelay);
        return $this;
    }

    protected function incrReleaseCounting(): int
    {
        return $this->client->getRedis()->incr($this->getRedisKeyForReleases());
    }

    protected function getRedisKeyForReleases(): string
    {
        return sprintf(static::$redisKeyForJobReleases, $this->uid);
    }

    protected function getStats(): ResponseInterface
    {
        return $this->client->getPheanstalk()->statsJob($this);
    }
}
