<?php

namespace Sai97\WebManAmqp;

use Carbon\Carbon;
use support\Container;
use Webman\Bootstrap;
use Workerman\Timer;
use Workerman\Worker;

class Heartbeat implements Bootstrap
{
    /**
     * @throws \Exception
     */
    public static function start(?Worker $worker)
    {
        try {
            if ($worker->name != "webman") return;

            //读取amqp配置文件
            $configs = config("plugin.sai97.webman-amqp.app");
            if (!isset($configs["enable"]) || $configs["enable"] !== true) return;
            //获取所有的连接配置
            if (!isset($configs["connections"]) || empty($connections = $configs["connections"])) return;

            /**
             * @var AmqpQueueService $service
             */
            $service = Container::get(AmqpQueueService::class);
            foreach ($connections as $connection) {
                $service->register(new $connection["instance"]);
            }
            //定时检查并发送心跳数据
//            Timer::add($config[""], function () use ($allQueueJobs) {
//                $connection->checkHeartBeat();
//                echo "[" . Carbon::now()->format("Y-m-d H:i:s") . "] RabbitMQ检测心跳状态机制执行完毕..." . PHP_EOL;
//            });
        } catch (\Throwable $throwable) {
            echo "AMQP心跳进程发生错误: class->" . __CLASS__ . ", error->{$throwable->getMessage()}, file->{$throwable->getFile()}, line->{$throwable->getLine()}" . PHP_EOL;
            return;
        }
    }
}