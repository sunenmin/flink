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
 * @Date 2021/3/27 3:00 下午
 * @Version 1.0
 **/
public class Test2_VoltageIncreaseAlert {
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
                .process(new VoltageIncreaseAlert(10)).print();

        env.execute();
    }

    public static class VoltageIncreaseAlert extends KeyedProcessFunction<Tuple, BatteryData, String>{

        private Integer threshold;
        private ValueState<Double> lastVoltageState;
        private ValueState<Long> tsTimerState;


        @Override
        public void processElement(BatteryData value, Context ctx, Collector<String> out) throws Exception {

            Double lastVoltage = lastVoltageState.value();
            Long tsTimer = tsTimerState.value();

            //如果电压增大且没有定时器，注册定时器
            if (value.getTotalVoltage() > lastVoltage && tsTimer == null){
                Long ts = ctx.timerService().currentProcessingTime() + threshold * 1000L;
                ctx.timerService().registerProcessingTimeTimer(ts);
                tsTimerState.update(ts);
            }

            //如果电压没有增大且定时器不为空，删除定时器
            else if (value.getTotalVoltage() <= lastVoltage && tsTimer != null){
                ctx.timerService().deleteEventTimeTimer(tsTimerState.value());
                tsTimerState.clear();
            }

            //更新电压值
            lastVoltageState.update(value.getTotalVoltage());

        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            out.collect(ctx.getCurrentKey().getField(0) + " " + threshold.toString() + "s内电压连续上升，触发报警！！");
            tsTimerState.clear();
        }



        @Override
        public void open(Configuration parameters) throws Exception {
            lastVoltageState = getRuntimeContext().getState(new ValueStateDescriptor<Double>("last-voltage", Double.class, Double.MIN_VALUE));
            tsTimerState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("ts-timer", Long.class));
        }

        @Override
        public void close() throws Exception {

        }

        public VoltageIncreaseAlert(Integer threshold) {
            this.threshold = threshold;
        }
    }
}
