package com.sun.source;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.io.InputStream;
import java.util.Properties;

/**
 * @Author Matt Sun
 * @Date 2021/3/20 8:11 下午
 * @Version 1.0
 **/
public class Test_Kafka {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();

        InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream("properties.properties");
        properties.load(in);

        DataStream<String> dataStream = env.addSource(new FlinkKafkaConsumer011<String>("inputdata", new SimpleStringSchema(), properties));

        dataStream.print();

        env.execute();

    }
}
