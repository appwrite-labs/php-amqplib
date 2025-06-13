<?php
namespace PhpAmqpLib\Connection;

use PhpAmqpLib\Wire\IO\SwooleIO;

class AMQPSwooleConnection extends AbstractConnection
{
    /**
     * @param string $host
     * @param int $port
     * @param string $user
     * @param string $password
     * @param string $vhost
     * @param bool $insist
     * @param string $login_method
     * @param string $locale
     * @param float $connection_timeout
     * @param float $read_write_timeout
     * @param resource|array|null $context
     * @param bool $keepalive
     * @param int $heartbeat
     * @param float $channel_rpc_timeout
     * @param AMQPConnectionConfig|null $config
     * @throws \Exception
     */
    public function __construct(
        $host,
        $port,
        $user,
        $password,
        $vhost = '/',
        $insist = false,
        $login_method = 'AMQPLAIN',
        $locale = 'en_US',
        $connection_timeout = 3.0,
        $read_write_timeout = 3.0,
        $context = null,
        $keepalive = false,
        $heartbeat = 0,
        $channel_rpc_timeout = 0.0,
        ?AMQPConnectionConfig $config = null
    ) {
        if ($channel_rpc_timeout > $read_write_timeout) {
            throw new \InvalidArgumentException('channel RPC timeout must not be greater than I/O read-write timeout');
        }

        $io = new SwooleIO(
            $host,
            $port,
            $connection_timeout,
            $read_write_timeout,
            $context,
            $keepalive,
            $heartbeat
        );

        parent::__construct(
            $user,
            $password,
            $vhost,
            $insist,
            $login_method,
            null, // login_response - deprecated but still needed for parent constructor
            $locale,
            $io,
            $heartbeat,
            $connection_timeout,
            $channel_rpc_timeout,
            $config
        );

        // save the params for the use of __clone, this will overwrite the parent
        $this->construct_params = func_get_args();
    }
}
