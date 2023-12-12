<?php

return [

    'enable' => true,

    "connections" => [
        "default" => [
            "instance" => \app\queue\DefaultQueue::class,
            "host" => env("AMQP_HOST", "127.0.0.1"),
            "port" => env("AMQP_PORT", 5672),
            "user" => env("AMQP_USER", "root"),
            "password" => env("AMQP_PASSWORD", "root"),
            "heartbeat" => 30
        ]
    ]
];