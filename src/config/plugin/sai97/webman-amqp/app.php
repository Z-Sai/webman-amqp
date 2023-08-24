<?php

use config\plugin\sai97\WebManAmqp\Job\DefaultQueueJob;

return [

    'enable' => true,

    "connection" => [
        "default" => [
            "host" => env("AMQP_HOST", "127.0.0.1"),
            "port" => env("AMQP_PORT", 5672),
            "user" => env("AMQP_USER", "root"),
            "password" => env("AMQP_PASSWORD", "root")
        ]
    ],

    "event" => [
        "default" => DefaultQueueJob::class,
    ]
];