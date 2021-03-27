package com.sun.processfunction;

import com.sun.models.BatteryData;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.io.InputStream;
import java.util.Properties;

/**
 * @Author Matt Sun
 * @Date 2021/3/27 5:49 下午
 * @Version 1.0
 **/
public class Test3_SplitStream {
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


        OutputTag<BatteryData> lowVoltage = new OutputTag<>("lowVoltage"){};
        SingleOutputStreamOperator<BatteryData> resultStream = dataStream.process(new ProcessFunction<BatteryData, BatteryData>() {
            @Override
            public void processElement(BatteryData value, Context ctx, Collector<BatteryData> out) throws Exception {

                if (value.getTotalVoltage() >= 320){
                    out.collect(value);
                }
                else {
                    ctx.output(lowVoltage, value);
                }
            }
        });

        resultStream.print("highVoltage:");
        resultStream.getSideOutput(lowVoltage).print("lowVoltage:");
        env.execute();
    }
}
