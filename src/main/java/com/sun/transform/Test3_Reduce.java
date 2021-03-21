package com.sun.transform;

import com.sun.models.BatteryData;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author Matt Sun
 * @Date 2021/3/21 7:05 上午
 * @Version 1.0
 **/
public class Test3_Reduce {
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

//        KeyedStream<BatteryData, String> keyedStream1 = dataStream.keyBy(data -> data.getVin());
        KeyedStream<BatteryData, String> keyedStream = dataStream.keyBy(BatteryData::getVin);

        //reduce聚合，根据vin号聚合
        DataStream<BatteryData> reduceStream = keyedStream.reduce(new ReduceFunction<BatteryData>() {
            @Override
            public BatteryData reduce(BatteryData value1, BatteryData value2) throws Exception {
                return new BatteryData(value1.getVin(), value2.getTimestamp(), Math.max(value1.getTotalVoltage(), value2.getTotalVoltage()),
                        Math.min(value1.getTotalCurrent(), value2.getTotalCurrent()), Short.valueOf("1"));
            }
        });

        reduceStream.print();

        env.execute();
    }
}
