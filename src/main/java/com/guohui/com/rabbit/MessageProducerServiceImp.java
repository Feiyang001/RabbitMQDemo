package com.guohui.com.rabbit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
@Service
public class MessageProducerServiceImp implements IMessageProducer {

    private Logger logger = LoggerFactory.getLogger(MessageProducerServiceImp.class);

    @Resource
    private AmqpTemplate amqpTemplate;

    @Override
    public void sendMessage(Object message) {

        logger.info("发送消息");

        logger.info("to send message:",message);

        amqpTemplate.convertAndSend("queueTestKey",message);

    }
}
