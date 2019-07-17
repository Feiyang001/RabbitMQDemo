package com.guohui.util;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;


/**
 * 获取连接的工具类
 * create by guohui
 */

public class ConnectionUtils {

    /**
     * 获取mq的连接
     * @return
     */
    public static Connection getConnection() throws IOException, TimeoutException {
        //定义一个连接工厂
        ConnectionFactory factory = new ConnectionFactory();
        //设置连接服务 服务地址，端口号
        factory.setHost("127.0.0.1");
        factory.setPort(5672);
        factory.setVirtualHost("/vhost_guohui");
        factory.setUsername("guohui");
        factory.setPassword("guohui");

        return factory.newConnection();
    }
}
