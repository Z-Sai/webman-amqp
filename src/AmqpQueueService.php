<?php

namespace Sai97\WebManAmqp;

use PhpAmqpLib\Connection\AMQPStreamConnection;
use PhpAmqpLib\Message\AMQPMessage;
use PhpAmqpLib\Wire\AMQPTable;
use PhpAmqpLib\Channel\AMQPChannel;

class AmqpQueueService
{
    /**
     * 连接管理集合
     * @var array $managers
     */
    protected $managers;

    /**
     * 当前连接
     * @var AMQPStreamConnection $connection
     */
    protected $connection;

    /**
     * 当前通道
     * @var AMQPChannel $channel
     */
    protected $channel;

    /**
     * 当前队列实例
     * @var QueueInterface $queueJob
     */
    protected $queueJob;

    /**
     * amqp配置信息
     * @var array $config
     */
    protected $config;

    public function __construct()
    {
        //加载amqp配置
        if (empty($config = config("plugin.sai97.webman-amqp.app"))) {
            throw new AmqpQueueException("configuration not found for amqp.");
        }
        $this->config = $config;
    }

    /**
     * @throws \Exception
     */
    public function register(QueueInterface $queueJob): void
    {
        $connectName = $queueJob->getConnectName();

        if (isset($this->managers[$connectName])) {
            throw new AmqpQueueException("manager {$connectName} is exists.");
        }

        if (!isset($this->config["connections"][$connectName]) || empty($config = $this->config["connections"][$connectName])) {
            throw new AmqpQueueException("No connection configuration named {$connectName} was found.");
        }

        $manager["connection"] = new AMQPStreamConnection(
            $config["host"],
            $config["port"],
            $config["user"],
            $config["password"],
            $config["vhost"] ?? "/",
            $config["insist"] ?? false,
            $config["login_method"] ?? "AMQPLAIN",
            $config["login_response"] ?? null,
            $config["locale"] ?? "en_US",
            $config["connection_timeout"] ?? 3.0,
            $config["read_write_timeout"] ?? 3.0,
            $config["context"] ?? null,
            $config["keepalive"] ?? false,
            $config["heartbeat"] ?? 0,
            $config["channel_rpc_timeout"] ?? 0.0,
            $config["ssl_protocol"] ?? null,
            $config["config"] ?? null
        );
        $manager["channel"] = $manager["connection"]->channel();
        $manager["queueJob"] = new $config["instance"];
        $this->managers[$connectName] = $manager;
    }

    /**
     * @throws \Exception
     */
    protected function initManager(string $name): void
    {
        $manager = $this->managers[$name];
        $this->connection = $manager["connection"];
        $this->channel = $manager["channel"];
        $this->queueJob = $manager["queueJob"];
    }

    public function getManagers(): array
    {
        return $this->managers;
    }

    /**
     * @throws AmqpQueueException
     * @throws \Exception
     */
    public function Connection(string $name): static
    {
        $this->initManager($name);
        return $this;
    }

    /**
     * 生产者发送消息
     * @return void
     * @throws AmqpQueueException
     */
    public function producer(string $body): void
    {
        if ($this->queueJob->isPublisherConfirm()) {

            //设置通道为确认模式
            $this->channel->confirm_select($this->queueJob->getConfirmSelectNowait());

            //发布者异步确认ACK回调函数
            if (!is_null($publisherConfirmsAckHandler = $this->queueJob->getPublisherConfirmsAckHandler())) {
                $this->channel->set_ack_handler($publisherConfirmsAckHandler);
            }
            //发布者异步确认NACK回调函数
            if (!is_null($publisherConfirmsNackHandler = $this->queueJob->getPublisherConfirmsNackHandler())) {
                $this->channel->set_nack_handler($publisherConfirmsNackHandler);
            }
        }

        $properties = [
            "content_type" => $this->queueJob->getContentType(),
            "delivery_mode" => $this->queueJob->getMessageDeliveryMode()
        ];
        $message = new AMQPMessage($body, $properties);

        //初始化策略
        $this->initStrategy("producer");

        if ($this->queueJob->isDelay() && $this->queueJob->getDelayTTL()) {
            $arguments = [
                "x-delay" => $this->queueJob->getDelayTTL()
            ];
            $message->set("application_headers", new AMQPTable($arguments));
        }

        //执行发布消息
        $this->channel->basic_publish(
            $message,
            $this->queueJob->getExchangeName(),
            $this->queueJob->getRoutingKey() ? $this->queueJob->getRoutingKey() : $this->queueJob->getQueueName()
        );

        if ($this->queueJob->isPublisherConfirm()) {
            //等待接收服务器的ack和nack
            $this->channel->wait_for_pending_acks($this->queueJob->getPublisherConfirmWaitTime());
        }
    }

    /**
     * 消费者处理接收并处理消息
     * @return void
     */
    public function consumer(): void
    {
        //当前消费者QOS相关配置
        $this->channel->basic_qos($this->queueJob->getQosPrefetchSize(), $this->queueJob->getQosPrefetchCount(), $this->queueJob->isQosGlobal());

        //初始化策略
        $this->initStrategy("consumer");

        $datetime = date("Y-m-d H:i:s", time());
        echo " [{$datetime}] ChannelId:{$this->channel->getChannelId()} Waiting for messages:\n";

        $this->channel->basic_consume(
            $this->queueJob->getQueueName(),
            $this->queueJob->getConsumerTag(),
            $this->queueJob->isConsumerNoLocal(),
            $this->queueJob->isAutoAck(),
            $this->queueJob->isConsumerExclusive(),
            $this->queueJob->isConsumerNowait(),
            $this->queueJob->getCallback(),
            ($this->queueJob->getConsumerTicket() > 0) ? $this->queueJob->getConsumerTicket() : null,
            new AMQPTable($this->queueJob->getConsumerArgs())
        );

        while ($this->channel->is_open()) {
            $this->channel->wait();
        }
    }

    /**
     * 初始化策略
     */
    private function initStrategy(string $caller): void
    {
        if (!in_array($caller, ["producer", "consumer"])) {
            throw new AmqpQueueException("initStrategy scene Params is Fail.");
        }

        if ($this->queueJob->getExchangeName() && $this->queueJob->getExchangeType()) { //使用交换器交互模型

            $this->handlerExchangeDeclare();

            //如果是消费者调用方
            if ($caller == "consumer") {

                $queueName = $this->handlerQueueDeclare();

                //获取队列绑定交换器的路由KEY,优先选择getQueueBindRoutingKey
                $routingKey = $this->queueJob->getQueueBindRoutingKey() ? $this->queueJob->getQueueBindRoutingKey() : $this->queueJob->getRoutingKey();

                //将队列绑定至交换器
                $this->channel->queue_bind($queueName, $this->queueJob->getExchangeName(), $routingKey);
            }
        } else { //不使用交换器交互模型
            $this->handlerQueueDeclare();
        }
    }

    /**
     * 处理声明交换器
     * @return void
     */
    private function handlerExchangeDeclare(): void
    {
        $exchangeType = $this->queueJob->getExchangeType();

        //交换器附加参数
        $exchangeArgument = $this->queueJob->getExchangeArgs();

        //延迟队列
        if ($this->queueJob->isDelay()) {
            $exchangeArgument = array_merge($exchangeArgument, [
                "x-delayed-type" => $exchangeType
            ]);
            $exchangeType = "x-delayed-message";
        }

        //初始化交换器
        $this->channel->exchange_declare(
            $this->queueJob->getExchangeName(),
            $exchangeType,
            $this->queueJob->isExchangePassive(),
            $this->queueJob->isExchangeDurable(),
            $this->queueJob->isExchangeAutoDelete(),
            $this->queueJob->isExchangeInternal(),
            $this->queueJob->isExchangeNowait(),
            new AMQPTable($exchangeArgument),
            ($this->queueJob->getExchangeTicket() > 0) ? $this->queueJob->getExchangeTicket() : null
        );
    }

    /**
     * 处理声明队列
     * @return string 队列名称
     */
    private function handlerQueueDeclare(): string
    {
        //queue附加参数
        $argument = $this->queueJob->getQueueArgs();

        //开启死信队列模式
        if ($this->queueJob->isDeadLetter() && $this->queueJob->getDeadLetterExchangeName() && $this->queueJob->getDeadLetterRoutingKey()) {
            //声明业务队列的死信交换器
            $argument = array_merge($argument, [
                "x-dead-letter-exchange" => $this->queueJob->getDeadLetterExchangeName(), //配置死信交换器
                "x-dead-letter-routing-key" => $this->queueJob->getDeadLetterRoutingKey(), //配置RoutingKey
            ]);
        }

        //声明队列
        list($queueName) = $this->channel->queue_declare(
            $this->queueJob->getQueueName(),
            $this->queueJob->isQueuePassive(),
            $this->queueJob->isQueueDurable(),
            $this->queueJob->isQueueExclusive(),
            $this->queueJob->isQueueAutoDelete(),
            $this->queueJob->isQueueNowait(),
            new AMQPTable($argument),
            ($this->queueJob->getQueueTicket() > 0) ? $this->queueJob->getQueueTicket() : null
        );

        return $queueName;
    }

    /**
     * 释放相关服务连接
     */
    public function close(): void
    {
        $this->closeChannel();
        $this->closeConnection();
    }

    /**
     * 关闭连接
     * @return void
     */
    public function closeConnection(): void
    {
        if ($this->connection instanceof AMQPStreamConnection) {
            $this->connection->close();
        }
        $this->connection = null;
    }

    /**
     * 关闭通道
     * @return void
     */
    public function closeChannel(): void
    {
        if ($this->channel instanceof AMQPChannel) {
            $this->channel->close();
        }
        $this->channel = null;
    }

    /**
     * 析构函数
     */
    public function __destruct()
    {
//        if ($this->queueJob instanceof QueueInterface && $this->queueJob->isPublisherConfirm()) {
//            $this->closeChannel();
//        }
    }
}
