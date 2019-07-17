package com.guohui.consummer;

import com.guohui.util.ConnectionUtils;
import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * 消费者
 * create by guohui
 */

public class Consumer {

    public static void main(String[] args) throws Exception {
        Consumer consumer = new Consumer();
        testSimpQueue();
    }

    /**
     *  The simplest thing that does something
     * @throws IOException
     * @throws TimeoutException
     */
    public static void testSimpQueue () throws IOException, TimeoutException {

        String QUEUE_NAME = "test";

        Connection connection = ConnectionUtils.getConnection();

        Channel channel = connection.createChannel();

        channel.queueDeclare(QUEUE_NAME,false,false,false,null);

        DefaultConsumer consumer = new DefaultConsumer(channel){
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                super.handleDelivery(consumerTag, envelope, properties, body);

                String msg = new String(body,"utf-8");
                System.out.println(msg);
            }
        };
        //监听队列
        channel.basicConsume(QUEUE_NAME,true,consumer);
    }


    /**
     * Work queues
     * 	流程:生产者-->队列队列-->消费者(1,2,3....)
     * 	Work queues模式下,多个消费者监听生产者,rabbitMQ采用轮询的方式
     * 	将消息是平均发送给消费者(activeMQ是抢占式,谁抢到归谁)
     *  应用场景：对于任务过重或任务较多情况使用工作队列可以提高任务处理的速度
     */
    public void testWorkQueue() throws IOException, TimeoutException {
        String QUEUE_NAME = "work_queue";

        Connection connection = ConnectionUtils.getConnection();

        Channel channel = connection.createChannel();

        channel.queueDeclare(QUEUE_NAME,false,false,false,null);

        DefaultConsumer consumer = new DefaultConsumer(channel){
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                super.handleDelivery(consumerTag, envelope, properties, body);

                String msg = new String(body,"utf-8");
                System.out.println(msg);
            }
        };
        //监听队列
        channel.basicConsume(QUEUE_NAME,true,consumer);
    }

    //publish?subscribe
    public void testPublishAndSubscribe() throws IOException, TimeoutException{
        String QUEUE_NAME = "public_subscribe_queue";
        String EXCHANGE_NAME = "public_subscribe";

        Connection connection = ConnectionUtils.getConnection();
        Channel channel = connection.createChannel();
        //声明队列
        channel.queueDeclare(QUEUE_NAME,false,false,false,null);
        //绑定交换机
        channel.queueBind(QUEUE_NAME, EXCHANGE_NAME, "");

        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

        DefaultConsumer consumer = new DefaultConsumer(channel){
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                super.handleDelivery(consumerTag, envelope, properties, body);

                String msg = new String(body,"utf-8");
                System.out.println(msg);
            }
        };
        //监听队列
        boolean autoAck = false;  //false 是自动应答
        channel.basicConsume(QUEUE_NAME,autoAck,consumer);
    }

    //routing
    public void testRouting() throws IOException, TimeoutException {
        String QUEUE_NAME = "routing_queue";
        String EXCHANGE_NAME = "test_routing";

        Connection connection = ConnectionUtils.getConnection();
        final Channel channel = connection.createChannel();
        //声明队列
        channel.queueDeclare(QUEUE_NAME,false,false,false,null);
        channel.basicQos(1);
        //绑定交换机
        channel.queueBind(QUEUE_NAME, EXCHANGE_NAME, "error");
        //channel.queueBind(QUEUE_NAME, EXCHANGE_NAME, "info");   绑定多个routingKey

        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

        DefaultConsumer consumer = new DefaultConsumer(channel){
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                super.handleDelivery(consumerTag, envelope, properties, body);

                String msg = new String(body,"utf-8");
                System.out.println(msg);
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }finally {
                    System.out.println("down");
                    channel.basicAck(envelope.getDeliveryTag(),false);
                }
            }
        };
        //监听队列
        boolean autoAck = false;  //false 是自动应答
        channel.basicConsume(QUEUE_NAME,autoAck,consumer);
    }

    //topic
    public void testTopic() throws IOException, TimeoutException {
        String QUEUE_NAME = "topic_queue";
        String EXCHANGE_NAME = "test_routing";

        Connection connection = ConnectionUtils.getConnection();
        final Channel channel = connection.createChannel();
        //声明队列
        channel.queueDeclare(QUEUE_NAME,false,false,false,null);
        channel.basicQos(1);
        //绑定交换机
        channel.queueBind(QUEUE_NAME, EXCHANGE_NAME, "goods.#"); //goods.add  goods.del goods.update .....
        //channel.queueBind(QUEUE_NAME, EXCHANGE_NAME, "info");   绑定多个routingKey

        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

        DefaultConsumer consumer = new DefaultConsumer(channel){
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                super.handleDelivery(consumerTag, envelope, properties, body);

                String msg = new String(body,"utf-8");
                System.out.println(msg);
                try {
                    Thread.sleep(3000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }finally {
                    System.out.println("down");
                    channel.basicAck(envelope.getDeliveryTag(),false);
                }
            }
        };
        //监听队列
        boolean autoAck = false;  //false 是自动应答
        channel.basicConsume(QUEUE_NAME,autoAck,consumer);
    }


}
