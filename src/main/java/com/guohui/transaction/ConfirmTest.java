package com.guohui.transaction;

import com.guohui.util.ConnectionUtils;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConfirmListener;
import com.rabbitmq.client.Connection;

import java.io.IOException;
import java.util.Collections;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.TimeoutException;

public class ConfirmTest {
    /**
     * 实现原理是 ：channel 进入到confirm模式之后，所有在该channel上发布的消息都会指派一个唯一的表示id
     * 从1开始，一旦消息被投到匹配的队列之后，rabbitMQ会有回执消息（包括id）发送给生产者，如果消息是持久化的
     * 会将消息写到磁盘后发出
     *
     * 优点在与 ：异步处理 不用等待
     * channel.confirmSelect();
     * 普通模式：每发一条消息都会调用 waitForConfirm是（）方法
     * 批量模式：发送一批消息后会调用 waitForConfirm是（）方法
     * 异步 ： 提供回调方法， channel 对象提供一个confirmListener() 回调方法只包含deliveryTag（
     *       当前channel发出的消息序号） 我们需要为每一个channel维护一个UNconfirm的消息序号集合（sortedSet）
     *       发一条数据集合元素加一，回调异常handleAck方法。集合删除相应的记录，
     *
     */

    public static void main(String[] args) {

    }
    //普通模式
    public void send1() throws IOException, TimeoutException, InterruptedException {
        String QUEUE_NAME = "confirm1_transaction";
        Connection connection = ConnectionUtils.getConnection();
        Channel channel = connection.createChannel();

        channel.queueDeclare(QUEUE_NAME, false, false, false, null);

        channel.confirmSelect();

        /*
         * for(int i= 0;i++;i<10){
         *   channel.basicPublish("", QUEUE_NAME, null, message.getBytes());
         * }
         *
         * 这个是批量的发送消息，发完消息之后再去确认
         */

        String message = "confirm 普通模式 test";
        channel.basicPublish("", QUEUE_NAME, null, message.getBytes());

        //确认
        if (!channel.waitForConfirms()){
            System.out.println("send message failed");
        }else {
            System.out.println("send message success");
        }
        channel.close();
        connection.close();
        System.out.println("===============信息分割===================");

    }


    //异步模式
    public void send2() throws IOException, TimeoutException, InterruptedException {
        String QUEUE_NAME = "confirm2_transaction";
        Connection connection = ConnectionUtils.getConnection();
        Channel channel = connection.createChannel();

        channel.queueDeclare(QUEUE_NAME, false, false, false, null);

        channel.confirmSelect();
        //存放为确认的消息
        final SortedSet<Long> confirmSet = Collections.synchronizedSortedSet(new TreeSet<Long>());

        //通道添加监听
        channel.addConfirmListener(new ConfirmListener() {

            //成功调用
            public void handleAck(long l, boolean b) throws IOException {
                if (b){
                    System.out.println("-----handleAck----b");
                    confirmSet.headSet(l+1).clear();
                }else {
                    System.out.println("-----handleAck----b false");
                    confirmSet.headSet(l);
                }
            }

            //失败调用
            public void handleNack(long l, boolean b) throws IOException {
                if (b){
                    System.out.println("-----handleNack----b");
                    confirmSet.headSet(l+1).clear();
                }else {
                    System.out.println("-----handleNack----b false");
                    confirmSet.headSet(l);
                }
            }
        });

        String message = "confirm 普通模式 test";

        while (true){
            long seqNo = channel.getNextPublishSeqNo();
            channel.basicPublish("", QUEUE_NAME, null, message.getBytes());
            confirmSet.add(seqNo);
        }

    }

}
