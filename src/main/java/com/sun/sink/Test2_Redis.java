package com.sun.sink;

import com.sun.models.BatteryData;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

/**
 * @Author Matt Sun
 * @Date 2021/3/21 12:10 下午
 * @Version 1.0
 **/
public class Test2_Redis {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStream<String> inputStream = env.readTextFile("/Users/mattsun/IdeaProjects/flink-study/src/main/resources/BatteryDate.txt");

        DataStream<BatteryData> dataStream = inputStream.map(new MapFunction<String, BatteryData>() {
            @Override
            public BatteryData map(String value) throws Exception {
                String[] fields = value.split(",");
                return new BatteryData(fields[0], Long.valueOf(fields[1]), Double.valueOf(fields[2]),
                        Double.valueOf(fields[3]), Short.valueOf(fields[4]));
            }
        });

        //定义jedis连接配置

        FlinkJedisPoolConfig flinkJedisPoolConfig = new FlinkJedisPoolConfig.Builder()
                .setHost("localhost")
                .setPort(6379)
                .build();

        dataStream.addSink(new RedisSink<>(flinkJedisPoolConfig, new MyRedisMapper()));

        env.execute();
    }

    //自动化RedisMapper
    public static class MyRedisMapper implements RedisMapper<BatteryData> {

        //定义保存到redis的操作,保存成Hash表 batterydata vim totalVoltage
        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.HSET, "batterydata");
        }

        @Override
        public String getKeyFromData(BatteryData data) {
            return data.getVin();
        }

        @Override
        public String getValueFromData(BatteryData data) {
            return data.getTotalVoltage().toString();
        }

    }
}
