<?php

include(__DIR__ . '/config.php');

use PhpAmqpLib\Connection\AMQPSwooleConnection;

go(function () {


    $exchange = 'exchange';
    $queue = 'queue2';
    $consumerTag = 'consumer';

//    $connection = new AMQPSocketConnection(HOST, PORT, USER, PASS, VHOST);
    $connection = new AMQPSwooleConnection(HOST, PORT, USER, PASS, VHOST);
    $channel = $connection->channel();

    /*
        The following code is the same both in the consumer and the producer.
        In this way we are sure we always have a queue to consume from and an
            exchange where to publish messages.
    */

    /*
        name: $queue
        passive: false
        durable: true // the queue will survive server restarts
        exclusive: false // the queue can be accessed in other channels
        auto_delete: false //the queue won't be deleted once the channel is closed.
    */
    $channel->queue_declare($queue, false, true, false, false);

    /*
        name: $exchange
        type: direct
        passive: false
        durable: true // the exchange will survive server restarts
        auto_delete: false //the exchange won't be deleted once the channel is closed.
    */

    $channel->exchange_declare($exchange, 'direct', false, true, false);

    $channel->queue_bind($queue, $exchange);

    /**
     * @param \PhpAmqpLib\Message\AMQPMessage $message
     */
    function process_message($message)
    {
        echo "\n--------\n";
        echo $message->getBody();
        echo "\n--------\n";

        $message->ack();

        // Send a message with the string "quit" to cancel the consumer.
        if ($message->getBody() === 'quit')
        {
            $message->getChannel()->basic_cancel($message->getConsumerTag());
        }
    }

    /*
        queue: Queue from where to get the messages
        consumer_tag: Consumer identifier
        no_local: Don't receive messages published by this consumer.
        no_ack: Tells the server if the consumer will acknowledge the messages.
        exclusive: Request exclusive consumer access, meaning only this consumer can access the queue
        nowait:
        callback: A PHP Callback
    */

    $channel->basic_consume($queue, $consumerTag, false, false, false, false, 'process_message');

    while (count($channel->callbacks))
    {
        $channel->wait();
    }
});
