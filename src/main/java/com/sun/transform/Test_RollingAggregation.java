package com.sun.transform;

import com.sun.models.BatteryData;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author Matt Sun
 * @Date 2021/3/20 10:40 下午
 * @Version 1.0
 **/
public class Test_RollingAggregation {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStream<String> inputStream = env.readTextFile("/Users/mattsun/IdeaProjects/flink-study/src/main/resources/BatteryDate.txt");

        //map转换成BatteryData
        DataStream<BatteryData> dataStream = inputStream.map(new MapFunction<String, BatteryData>() {
            @Override
            public BatteryData map(String value) throws Exception {
                String[] fields = value.split(",");
                BatteryData batteryData = new BatteryData();
                batteryData.setVin(fields[0]);
                batteryData.setTimestamp(Long.valueOf(fields[1]));
                batteryData.setTotalVoltage(Double.valueOf(fields[2]));
                batteryData.setTotalCurrent(Double.valueOf(fields[3]));
                batteryData.setVehicleChrgSts(Short.valueOf(fields[4]));
                return batteryData;
            }
        });

        //通过keyBy进行分组
        KeyedStream<BatteryData, Tuple> keyedStream = dataStream.keyBy("vin");

        DataStream<BatteryData> resultStream = keyedStream.maxBy("totalVoltage");

        resultStream.print();

        env.execute();

    }
}
