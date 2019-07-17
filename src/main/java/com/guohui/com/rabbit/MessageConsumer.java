package com.guohui.com.rabbit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.plugin2.message.Message;

public class MessageConsumer implements MessageListener {
    private Logger logger = LoggerFactory.getLogger(MessageConsumer.class);

    @Override
    public void onMessage(Message message) {

        logger.info("------消费者处理消息------");

        logger.info("receive message",message);

    }
}
