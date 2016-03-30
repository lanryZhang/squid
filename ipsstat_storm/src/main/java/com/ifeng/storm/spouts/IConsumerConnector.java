package com.ifeng.storm.spouts;

import kafka.javaapi.consumer.ConsumerConnector;

import java.io.Serializable;

/**
 * Created by zhanglr on 2016/3/30.
 */
public interface IConsumerConnector extends Serializable ,ConsumerConnector{
}
