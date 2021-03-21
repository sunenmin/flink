package com.sun.source;

import com.sun.models.BatteryData;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

/**
 * @Author Matt Sun
 * @Date 2021/3/20 8:38 下午
 * @Version 1.0
 **/
public class Test4_User_Defined {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<BatteryData> dataStream = env.addSource(new BatteryDataGenerator());

        dataStream.print();

        env.execute();
    }

    public static class BatteryDataGenerator implements SourceFunction<BatteryData> {

        private boolean running = true;
        @Override
        public void run(SourceContext<BatteryData> sourceContext) throws Exception {
            Random random = new Random();
            while (running){
                for (int var1 = 0; var1 < 10; var1 ++){
                    BatteryData batteryData = new BatteryData();
                    batteryData.setVin("LGWWCM00" + (var1 + 1));
                    batteryData.setTimestamp(System.currentTimeMillis());
                    batteryData.setTotalVoltage(321.23 + (random.nextGaussian() * 10));
                    batteryData.setTotalCurrent(50 + (random.nextGaussian()*10));
                    batteryData.setVehicleChrgSts(Short.valueOf(random.nextInt(2) + ""));
                    sourceContext.collect(batteryData);

                    Thread.sleep(1000);
                }
            }

        }

        @Override
        public void cancel() {
            this.running = false;
        }
    }
}

