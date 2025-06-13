<?php

namespace PhpAmqpLib\Wire\IO;

use PhpAmqpLib\Exception\AMQPConnectionClosedException;
use PhpAmqpLib\Exception\AMQPIOException;
use PhpAmqpLib\Exception\AMQPRuntimeException;
use PhpAmqpLib\Exception\AMQPTimeoutException;
use Swoole\Coroutine\Client;

class SwooleIO extends AbstractIO
{
    /** @var Client|null */
    private $sock;

    /** @var string */
    private $buffer = '';

    /**
     * @param string $host
     * @param int $port
     * @param float $connection_timeout
     * @param float $read_write_timeout
     * @param null $context
     * @param bool $keepalive
     * @param int $heartbeat
     */
    public function __construct(
        $host,
        $port,
        $connection_timeout,
        $read_write_timeout,
        $context = null,
        $keepalive = false,
        $heartbeat = 0
    ) {
        if ($heartbeat !== 0 && ($read_write_timeout < ($heartbeat * 2))) {
            throw new \InvalidArgumentException('read_write_timeout must be at least 2x the heartbeat');
        }
        $this->host = $host;
        $this->port = $port;
        $this->connection_timeout = $connection_timeout;
        $this->read_timeout = $read_write_timeout;
        $this->write_timeout = $read_write_timeout;
        $this->keepalive = $keepalive;
        $this->heartbeat = $heartbeat;
        $this->initial_heartbeat = $heartbeat;
        $this->canDispatchPcntlSignal = false; // Swoole handles signals differently
    }

    /**
     * Sets up the stream connection
     *
     * @throws AMQPRuntimeException
     * @throws AMQPIOException
     */
    public function connect()
    {
        $this->sock = new Client(SWOOLE_SOCK_TCP);

        // Set socket options before connecting
        $this->sock->set([
            'timeout' => $this->connection_timeout,
            'connect_timeout' => $this->connection_timeout,
            'write_timeout' => $this->write_timeout,
            'read_timeout' => $this->read_timeout,
            'open_tcp_nodelay' => true,
            'tcp_keepalive' => $this->keepalive,
            'package_max_length' => 2 * 1024 * 1024, // 2MB max package
        ]);

        if (!$this->sock->connect($this->host, $this->port)) {
            throw new AMQPIOException(
                sprintf(
                    'Error Connecting to server(%s): %s ',
                    $this->sock->errCode,
                    swoole_strerror($this->sock->errCode)
                ),
                $this->sock->errCode
            );
        }
    }

    /**
     * @param int $len
     * @return string
     * @throws AMQPIOException
     * @throws AMQPRuntimeException
     * @throws AMQPTimeoutException
     * @throws AMQPConnectionClosedException
     */
    public function read($len)
    {
        if ($this->sock === null) {
            throw new AMQPConnectionClosedException('Socket connection is closed');
        }

        $this->check_heartbeat();

        $data = '';
        $remaining = $len;

        // First, consume from buffer
        if ($this->buffer !== '') {
            $chunk_size = min($remaining, strlen($this->buffer));
            $data = substr($this->buffer, 0, $chunk_size);
            $this->buffer = substr($this->buffer, $chunk_size);
            $remaining -= $chunk_size;
        }

        // Read remaining bytes from socket
        while ($remaining > 0) {
            if (!$this->sock->connected) {
                throw new AMQPConnectionClosedException('Broken pipe or closed connection');
            }

            // Swoole recv() returns false on error, empty string on EOF
            $chunk = $this->sock->recv($remaining, $this->read_timeout);

            if ($chunk === false) {
                if ($this->sock->errCode == SOCKET_ETIMEDOUT) {
                    throw new AMQPTimeoutException('Read timeout');
                }
                throw new AMQPIOException(
                    sprintf('Error receiving data: %s', swoole_strerror($this->sock->errCode)),
                    $this->sock->errCode
                );
            }

            if ($chunk === '') {
                throw new AMQPConnectionClosedException('Connection closed by peer');
            }

            $data .= $chunk;
            $remaining -= strlen($chunk);
        }

        $this->last_read = microtime(true);
        return $data;
    }

    /**
     * @param string $data
     * @throws AMQPRuntimeException
     * @throws AMQPTimeoutException
     * @throws AMQPConnectionClosedException
     */
    public function write($data)
    {
        if ($this->sock === null || !$this->sock->connected) {
            throw new AMQPConnectionClosedException('Socket connection is closed');
        }

        $this->checkBrokerHeartbeat();

        // Swoole send() handles partial writes internally
        $result = $this->sock->send($data);

        if ($result === false) {
            if ($this->sock->errCode == SOCKET_ETIMEDOUT) {
                throw new AMQPTimeoutException('Write timeout');
            }
            throw new AMQPIOException(
                sprintf('Error sending data: %s', swoole_strerror($this->sock->errCode)),
                $this->sock->errCode
            );
        }

        if ($result !== strlen($data)) {
            throw new AMQPIOException('Could not write entire buffer');
        }

        $this->last_write = microtime(true);
    }

    /**
     * @return void
     */
    public function close()
    {
        if ($this->sock !== null && $this->sock->connected) {
            $this->sock->close();
        }
        $this->sock = null;
        $this->last_read = null;
        $this->last_write = null;
        $this->buffer = '';
    }

    /**
     * @param int|null $sec
     * @param int $usec
     * @return int|bool
     * @throws AMQPConnectionClosedException
     */
    protected function do_select(?int $sec, int $usec)
    {
        if ($this->sock === null || !$this->sock->connected) {
            throw new AMQPConnectionClosedException('Socket connection is closed');
        }

        // If we have buffered data, return immediately
        if (strlen($this->buffer) > 0) {
            return 1;
        }

        // Convert timeout to seconds for Swoole (supports fractional seconds)
        $timeout = $sec === null ? -1 : ($sec + $usec / 1000000);

        // Swoole doesn't have a true select() equivalent for coroutines.
        // We must use recv() to wait for data, then buffer it.
        // This blocks the coroutine (not the process) until data arrives or timeout.
        $data = $this->sock->recv($timeout);

        if ($data === false) {
            if ($this->sock->errCode == SOCKET_ETIMEDOUT) {
                return 0; // Timeout - no data available
            }
            // Connection error
            if ($this->sock->errCode == SOCKET_ECONNRESET || !$this->sock->connected) {
                throw new AMQPConnectionClosedException('Connection reset by peer');
            }
            return false; // Other error
        }

        if ($data === '') {
            throw new AMQPConnectionClosedException('Connection closed by peer');
        }

        // Buffer the received data for subsequent read() calls
        $this->buffer .= $data;
        return 1; // Data is now available
    }

    /**
     * @return Client|null
     */
    public function getSocket()
    {
        return $this->sock;
    }
}
