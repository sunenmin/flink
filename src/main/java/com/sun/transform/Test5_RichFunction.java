package com.sun.transform;

import com.sun.models.BatteryData;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author Matt Sun
 * @Date 2021/3/21 8:30 上午
 * @Version 1.0
 **/
public class Test5_RichFunction {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStream<String> dataStream = env.readTextFile("/Users/mattsun/IdeaProjects/flink-study/src/main/resources/BatteryDate.txt");

        DataStream<BatteryData> mapStream = dataStream.map(new MapFunction<String, BatteryData>() {
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

        DataStream<Tuple2<String, String>> richStream = mapStream.map(new MyMapFunction());

        richStream.print();

        env.execute();

    }

    public static class MyMapFunction extends  RichMapFunction<BatteryData, Tuple2<String, String>> {

        @Override
        public Tuple2<String, String> map(BatteryData value) throws Exception {
            return new Tuple2<String, String>(getRuntimeContext().getIndexOfThisSubtask() + "", value.getVin());
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            // 初始化工作，一般是定义状态，或者建立数据库连接
            System.out.println("map is open");
        }

        @Override
        public void close() throws Exception {
            // 一般是关闭连接和清空状态的收尾操作
            System.out.println("map is close");
        }
    }


}
