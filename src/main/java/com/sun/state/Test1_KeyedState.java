package com.sun.state;

import com.sun.jersey.core.header.OutBoundHeaders;
import com.sun.models.BatteryData;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.lucene.document.DoubleRange;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * @Author Matt Sun
 * @Date 2021/3/26 8:29 下午
 * @Version 1.0
 **/
public class Test1_KeyedState {
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

        SingleOutputStreamOperator<BatteryData> outputStream = dataStream.keyBy("vin")
                .map(new MyKeyCountMapper());

        outputStream.print();
        env.execute();
    }

    public static class MyKeyCountMapper extends RichMapFunction<BatteryData, BatteryData>{

        ValueState<BatteryData> batteryDataValueState;
        ValueState<List> voltageList;
        ListState<Double> listState;
        MapState<String, Integer> mapState;
        MapState<String, MapState<Integer, Double>> mapState1;

        @Override
        public void open(Configuration parameters) throws Exception {
            batteryDataValueState = getRuntimeContext().getState(new ValueStateDescriptor<BatteryData>("batteryStatus", BatteryData.class));
            voltageList = getRuntimeContext().getState(new ValueStateDescriptor<List>("voltageList", List.class));
            listState = getRuntimeContext().getListState(new ListStateDescriptor<Double>("my-list", Double.class));
            mapState = getRuntimeContext().getMapState(new MapStateDescriptor<String, Integer>("my-map", String.class, Integer.class));
        }


        @Override
        public BatteryData map(BatteryData value) throws Exception {


            List<Double> list = voltageList.value();
            BatteryData batteryData = batteryDataValueState.value();

            if (batteryData == null){
                batteryData = new BatteryData("", 0L, 0.0, 0.0,(short)0);
            }

            if (list == null){
                list = new ArrayList<>();
            }
            batteryData.setVin(value.getVin());
            batteryData.setTimestamp(value.getTimestamp());
            batteryData.setTotalVoltage(batteryData.getTotalVoltage() + value.getTotalVoltage());
            batteryData.setTotalCurrent(batteryData.getTotalCurrent() + value.getTotalCurrent());
            batteryData.setVehicleChrgSts(batteryData.getVehicleChrgSts());
            list.add(value.getTotalVoltage());
            list.add(value.getTotalVoltage() * 50);

            System.out.println("voltageList = " + list);
            voltageList.update(list);

            batteryDataValueState.update(batteryData);
            return batteryData;
        }
    }
}
