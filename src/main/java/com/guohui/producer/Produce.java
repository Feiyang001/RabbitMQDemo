package com.guohui.producer;

import com.guohui.util.ConnectionUtils;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * 生产者
 * create by guohui
 */
public class Produce {

    public static void main(String[] args) throws Exception {

        Produce produce = new Produce();
        testSimpleQueue();
    }

    /**
     * The simplest thing that does something  "Hello world"
     *
     * @throws IOException
     * @throws TimeoutException
     */
    public static void testSimpleQueue() throws Exception {

        String QUEUE_NAME = "test";

        Connection connection = ConnectionUtils.getConnection();

        Channel channel = connection.createChannel();

        //声明队列,参数1:队列名称,参数2:是否持久化,参数3:队列是否独占此连接,参数4:队列不再使用时是否自动删除此队列,参数5:队列参数
        channel.queueDeclare(QUEUE_NAME, false, false, false, null);

        String msg = "hello rabbitMQ!";

        //参数1:Exchange的名称，如果没有指定，则使用Default Exchange,
        // 参数2:routingKey,消息的路由Key，是用于Exchange（交换机）
        // 将消息转发到指定的消息队列,参数3:消息包含的属性,参数4:消息体
        channel.basicPublish("", QUEUE_NAME, null, msg.getBytes());

        System.out.println("send message success");

        channel.close();
        connection.close();
    }

    /**
     * Work queues
     * 流程:生产者-->队列-->默认交换机-->队列-->消费者
     * Work queues模式下,多个消费者监听生产者,rabbitMQ采用轮询的方式
     * 将消息是平均发送给消费者(activeMQ是抢占式,谁抢到归谁)
     * 应用场景：对于任务过重或任务较多情况使用工作队列可以提高任务处理的速度
     */
    public void testWorkQueue() throws IOException, TimeoutException {

        String QUEUE_NAME = "work_queue";

        Connection connection = ConnectionUtils.getConnection();
        Channel channel = connection.createChannel();
        //声明队列
        channel.queueDeclare(QUEUE_NAME, false, false, false, null);
        String msg = "this is a work queue message";
        //发送消息
        channel.basicPublish("", QUEUE_NAME, null, msg.getBytes());
        System.out.println("send message success");
        channel.close();
        connection.close();
    }

    /**
     * Publish/Subscribe
     * 交换机没有存储的能力，只有队列有存储的能力
     * 流程:生产者-->自定义交换机-->队列-->消费者
     * 发布订阅模式下,每个消费者监听自己的队列。生产者将消息发给broker
     * 由交换机将消息转发到绑定此交换机的每个队列，每个绑定交换机的队列都将接收到各自的消息
     * 应用场景：用户通知，当用户充值成功或转账完成系统通知用户，通知方式有短信、邮件多种方法。
     */
    public void testPublishAndSubscribe() throws IOException, TimeoutException {

        String EXCHANGE_NAME = "public_subscribe";

        Connection connection = ConnectionUtils.getConnection();
        Channel channel = connection.createChannel();
        //声明交换机
        channel.exchangeDeclare(EXCHANGE_NAME, "fanout");
        //发送消息
        String message = "hello PS";
        channel.basicPublish(EXCHANGE_NAME, "", null, message.getBytes("UTF-8"));
        System.out.println(" Sent massge is '" + message + "'");

        channel.close();
        connection.close();
    }

    /**
     * exchange 一方面是介绍p的消息，一方面是把消息发送到队列中
     * exchange 的类型
     * "" ：匿名转发
     * fanout ：不处理路由键 只要发送消息到交换机 就会把消息发送到与交换机绑定的所有的队列
     *direct ：处理路由键  由指定的routingKey 来对接相应的消息接收
     *
     */
    public void testRouting() throws IOException, TimeoutException {

        String EXCHANGE_NAME = "test_routing";

        Connection connection = ConnectionUtils.getConnection();
        Channel channel = connection.createChannel();
        //声明交换机
        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.DIRECT);
        //发送消息
        String message = "hello routing!!";
        String routingKey="error";
        channel.basicPublish(EXCHANGE_NAME, routingKey, null, message.getBytes("UTF-8"));
        System.out.println(" Sent message is '" + message + "'");
        channel.close();
        connection.close();
    }

    /**
     * 将路由键与某种模式进行匹配 ，队列需要绑定在一个模式上，用符号来表示  # 匹配一个或者多个   * 匹配一个
     *
     *
     */
    public void testTopic() throws IOException, TimeoutException{
        String EXCHANGE_NAME = "test_topic";

        Connection connection = ConnectionUtils.getConnection();
        Channel channel = connection.createChannel();
        //声明交换机
        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.TOPIC);
        //发送消息
        String message = "hello topic,add goods";
        String routingKey="goods.add";
        channel.basicPublish(EXCHANGE_NAME, routingKey, null, message.getBytes("UTF-8"));
        System.out.println(" Sent message is '" + message + "'");
        channel.close();
        connection.close();
    }



}