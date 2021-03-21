package com.sun.transform;

import com.sun.models.BatteryData;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

import java.util.Collections;

/**
 * @Author Matt Sun
 * @Date 2021/3/21 7:23 上午
 * @Version 1.0
 **/
public class Test4_MultipleStream {
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

        SplitStream<BatteryData> splitStream = dataStream.split(new OutputSelector<BatteryData>() {
            @Override
            public Iterable<String> select(BatteryData value) {
                return Collections.singleton(value.getVin());
            }
        });

        DataStream<BatteryData> vin1 = splitStream.select("LGWWCM003");
        DataStream<BatteryData> vin2 = splitStream.select("LGWWCM004");

        //connect操作；将vin为003的转换成二元组的形式，然后和vin为004的合并，输出vin为003的正常，vin为004的报警
        DataStream<Tuple2<String, Double>> vin003 = vin1.map(new MapFunction<BatteryData, Tuple2<String, Double>>() {
            @Override
            public Tuple2<String, Double> map(BatteryData value) throws Exception {
                return new Tuple2<>(value.getVin(), Double.valueOf(value.getTotalVoltage()));
            }
        });

        ConnectedStreams<Tuple2<String, Double>, BatteryData> connectedStreams = vin003.connect(vin2);

        DataStream<Tuple2<String, String>> comapStream = connectedStreams.map(new CoMapFunction<Tuple2<String, Double>, BatteryData, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> map1(Tuple2<String, Double> value) throws Exception {
                return new Tuple2<>(value.f0, "高压报警");
            }

            @Override
            public Tuple2<String, String> map2(BatteryData value) throws Exception {
                return new Tuple2<>(value.getVin(), "正常着呢！");
            }
        });

        comapStream.print();



//
//        DataStream<BatteryData> combineStream = vin1.union(vin2);
//
//
//        vin1.print();
//        vin2.print();
//        combineStream.print("combineStream:");

        env.execute();

    }
}
