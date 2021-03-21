package com.sun.sink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * @Author Matt Sun
 * @Date 2021/3/21 10:13 上午
 * @Version 1.0
 **/
public class Test1_Kafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

//        DataStream<String> inputStream = env.readTextFile("/Users/mattsun/IdeaProjects/flink-study/src/main/resources/BatteryDate.txt");

        Properties properties = new Properties();
        InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream("properties.properties");

        properties.load(in);

        DataStream<String> inputStream = env.addSource(new FlinkKafkaConsumer011<String>("inputdata", new SimpleStringSchema(), properties));

        inputStream.addSink(new FlinkKafkaProducer011<String>("localhost:9092", "outputdata", new SimpleStringSchema()));

        env.execute();


    }
}
