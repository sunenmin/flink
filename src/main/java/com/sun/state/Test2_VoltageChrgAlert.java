package com.sun.state;

import com.sun.models.BatteryData;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;

import java.io.InputStream;
import java.util.Properties;

/**
 * @Author Matt Sun
 * @Date 2021/3/27 9:22 上午
 * @Version 1.0
 **/
public class Test2_VoltageChrgAlert {
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

        SingleOutputStreamOperator<Tuple3<String, Double, Double>> outputStream = dataStream.keyBy("vin")
                .flatMap(new VoltageChrg(10.0));

        outputStream.print();
        env.execute();
    }

    public static class VoltageChrg extends RichFlatMapFunction<BatteryData, Tuple3<String, Double, Double>> {

        private double thresheld;
        private ValueState<Double> lastVoltage;
        public VoltageChrg(double threshold) {
            this.thresheld = threshold;
        }

        @Override
        public void flatMap(BatteryData value, Collector<Tuple3<String, Double, Double>> out) throws Exception {

            Double lastTotalVoltage = lastVoltage.value();
            Double diff;
            if (lastTotalVoltage != null){
                diff = Math.abs(value.getTotalVoltage() - lastTotalVoltage);
                if (diff >= thresheld){
                    out.collect(new Tuple3<String, Double, Double>(value.getVin(), lastTotalVoltage, value.getTotalVoltage()));
                }
            }
            lastVoltage.update(value.getTotalVoltage());

        }

        @Override
        public void open(Configuration parameters) throws Exception {
            lastVoltage = getRuntimeContext().getState(new ValueStateDescriptor<Double>("voltageState", Double.class));
        }


        @Override
        public void close() throws Exception {
            lastVoltage.clear();
        }

    }

}
