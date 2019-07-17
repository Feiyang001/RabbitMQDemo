package com.guohui.transaction;

import com.guohui.util.ConnectionUtils;
import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class TransactionTest {

    /**
     * 事物transaction机制 和confirm
     * 在rabbitMQ中可以通过消息持久化来解决服务器异常时防止消息丢失
     * 如果不做相应的设置。是不清楚 p 发送的消息是否到达了 服务器
     * 解决的方法 ：1 通过 AMQP 协议模式 （事物机制） txSelect（)把当前的channel设置为事物模式 txCommit() txRollback(); 降低了消息的吞吐量
     * 2 confirm （推荐 ）
     */

    public static void main(String[] args) throws IOException, TimeoutException {
        TransactionTest test = new TransactionTest();
        test.send();
    }

    public void send() throws IOException, TimeoutException {
        String QUEUE_NAME = "test_transaction";
        Connection connection = ConnectionUtils.getConnection();
        Channel channel = connection.createChannel();

        try {
            channel.queueDeclare(QUEUE_NAME, false, false, false, null);

            channel.txSelect();
            String msge = "transaction test";
            channel.basicPublish("", QUEUE_NAME, null, msge.getBytes());
            channel.txCommit();
        } catch (Exception e) {
            channel.txRollback();
            System.out.println("send message failed");
        } finally {
            channel.close();
            connection.close();

            System.out.println("===============信息分割===================");
        }
    }
}

