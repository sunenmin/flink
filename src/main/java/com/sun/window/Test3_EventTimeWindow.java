package com.sun.window;

import com.sun.models.BatteryData;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.OutputTag;

import java.io.InputStream;
import java.util.Properties;

/**
 * @Author Matt Sun
 * @Date 2021/3/24 7:10 下午
 * @Version 1.0
 **/
public class Test3_EventTimeWindow {
    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(100);

        Properties properties = new Properties();
        InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream("properties.properties");

        properties.load(in);

        FlinkKafkaConsumer011<String> consumer = new FlinkKafkaConsumer011<String>("inputdata", new SimpleStringSchema(), properties);

        DataStreamSource<String> inputStream = env.addSource(consumer);

        DataStream<BatteryData> dataStream = inputStream.map(new MapFunction<String, BatteryData>() {
            @Override
            public BatteryData map(String value) throws Exception {
                String[] fields = value.split(",");
                return new BatteryData(fields[0], Long.valueOf(fields[1]), Double.valueOf(fields[2]),
                        Double.valueOf(fields[3]), Short.valueOf(fields[4]));
            }
        });

        //升序数据设置事件时间和WaterMark
//        dataStream.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<BatteryData>() {
//            @Override
//            public long extractAscendingTimestamp(BatteryData element) {
//                return element.getTimestamp() * 1000L;
//            }
//        });

        //乱序数据设置事件时间和WaterMark
        DataStream<BatteryData> resultStream = dataStream
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<BatteryData>(Time.seconds(5)) {
                    @Override
                    public long extractTimestamp(BatteryData element) {
                        return element.getTimestamp() * 1000L;
                    }
                });

        OutputTag<BatteryData> outputTag = new OutputTag<>("late"){};
        //基于事件时间的开窗聚合
        SingleOutputStreamOperator<BatteryData> minTotalVoltage = resultStream.keyBy("vin")
                .timeWindow(Time.seconds(15))
                .allowedLateness(Time.minutes(1))
                .sideOutputLateData(outputTag)
                .minBy("totalVoltage");

        minTotalVoltage.print();

        minTotalVoltage.getSideOutput(outputTag).print("late");


        env.execute();

    }
}
