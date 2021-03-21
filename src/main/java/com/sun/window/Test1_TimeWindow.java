package com.sun.window;

import com.sun.models.BatteryData;
import com.sun.source.Test4_User_Defined;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.table.runtime.aggregate.AggregateAggFunction;

import java.io.InputStream;
import java.util.Properties;

/**
 * @Author Matt Sun
 * @Date 2021/3/21 3:53 下午
 * @Version 1.0
 **/
public class Test1_TimeWindow {
    public static void main(String[] args)  throws  Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

//        DataStream<BatteryData> dataStream = env.addSource(new Test4_User_Defined.BatteryDataGenerator());

        Properties properties = new Properties();

        InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream("properties.properties");
        properties.load(in);

        DataStream<String> inputStream = env.addSource(new FlinkKafkaConsumer011<String>("inputdata", new SimpleStringSchema(), properties));

        DataStream<BatteryData> dataStream = inputStream.map(new MapFunction<String, BatteryData>() {
            @Override
            public BatteryData map(String value) throws Exception {
                String[] fields = value.split(",");
                return new BatteryData(fields[0], Long.valueOf(fields[1]), Double.valueOf(fields[2]),
                        Double.valueOf(fields[3]), Short.valueOf(fields[4]));
            }
        });


        DataStream<Integer> resultStream = dataStream.keyBy("vin")
                .timeWindow(Time.seconds(15))
                .aggregate(new AggregateFunction<BatteryData, Integer, Integer>() {
                    @Override
                    public Integer createAccumulator() {
                        return 0;
                    }

                    @Override
                    public Integer add(BatteryData value, Integer accumulator) {
//                        if (value.getTotalVoltage() >= 320){
//                            return accumulator += 1;
//                        }
                        return accumulator + 1;
                    }

                    @Override
                    public Integer getResult(Integer accumulator) {
                        return accumulator;
                    }

                    @Override
                    public Integer merge(Integer a, Integer b) {
                        return a + b;
                    }
                });


        resultStream.print();

        env.execute();
    }
}
