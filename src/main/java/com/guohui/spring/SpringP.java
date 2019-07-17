package com.guohui.spring;

import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

public class SpringP {
    public static void main(String[] args) {
        AbstractApplicationContext ac = new ClassPathXmlApplicationContext("classpath:applicationContext.xml");

        //rabbit 的模版
        RabbitTemplate rb = ac.getBean(RabbitTemplate.class);
        //发送消息
        rb.convertAndSend("hello");

        ac.destroy();//销毁容器
    }
}
