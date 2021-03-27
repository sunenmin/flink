package com.sun.processfunction;

import com.sun.models.BatteryData;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;

import java.io.InputStream;
import java.util.Properties;

/**
 * @Author Matt Sun
 * @Date 2021/3/27 12:08 下午
 * @Version 1.0
 **/
public class Test1_KeyedProcessFunction {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        Properties properties = new Properties();
        InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream("properties.properties");

        properties.load(in);

        FlinkKafkaConsumer011 consumer = new FlinkKafkaConsumer011("inputdata",new SimpleStringSchema(), properties);


        DataStreamSource<String> inputStream = env.addSource(consumer);

        DataStream<BatteryData> dataStream = inputStream.map(new MapFunction<String, BatteryData>() {
            @Override
            public BatteryData map(String value) throws Exception {
                String[] fields = value.split(",");
                return new BatteryData(fields[0], Long.valueOf(fields[1]), Double.valueOf(fields[2]),
                        Double.valueOf(fields[3]), Short.valueOf(fields[4]));
            }
        });

        dataStream.keyBy("vin")
                .process(new MyKeyedProcess())
        .print();

        env.execute();
    }

    public static class MyKeyedProcess extends KeyedProcessFunction<Tuple, BatteryData, Integer>{

        private ValueState<Integer> count;
        private ValueState<Long> tsTimer;

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Integer> out) throws Exception {
            System.out.println(timestamp + ":定时器被出发啦！！！！");
        }

        @Override
        public void processElement(BatteryData value, Context ctx, Collector<Integer> out) throws Exception {
            int totalCount = count.value();
            out.collect(totalCount + 1);
            count.update(totalCount + 1);

            ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime() + 5000L);
            tsTimer.update(ctx.timerService().currentProcessingTime() + 5000L);

//            ctx.timerService().deleteEventTimeTimer(tsTimer.value());

        }

        @Override
        public void open(Configuration parameters) throws Exception {
            count = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("count" , Integer.class, 0));

            tsTimer = getRuntimeContext().getState(new ValueStateDescriptor<Long>("tsTimer", Long.class));
        }

        @Override
        public void close() throws Exception {
            tsTimer.clear();
            count.clear();
        }
    }
}
