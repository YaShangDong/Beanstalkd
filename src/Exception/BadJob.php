<?php declare(strict_types=1);

namespace YaSD\Beanstalkd\Exception;

use RuntimeException;
use Throwable;
use YaSD\Beanstalkd\Exception;

class BadJob extends RuntimeException implements Exception
{
    public static function forInvalidData(string $jobData, ?Throwable $previous = null): static
    {
        $msg = sprintf('Invalid job data format: %s', $jobData);
        return new static($msg, 0, $previous);
    }
}
