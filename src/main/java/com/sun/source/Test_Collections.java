package com.sun.source;

import com.sun.models.BatteryData;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * @Author Matt Sun
 * @Date 2021/3/20 7:11 下午
 * @Version 1.0
 **/
public class Test_Collections {

    public static void main(String[] args) throws Exception {
        //1.创建流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //2.source：从集合中读取数据
        DataStream<BatteryData> dataStream = env.fromCollection(
                Arrays.asList(
                        new BatteryData("LGWWCM001", System.currentTimeMillis(), 321.25, 45.20, new Short("0")),
                        new BatteryData("LGWWCM002", System.currentTimeMillis(), 312.34, 45.20, new Short("0")),
                        new BatteryData("LGWWCM003", System.currentTimeMillis(), 334.23, 32.45, new Short("1")),
                        new BatteryData("LGWWCM004", System.currentTimeMillis(), 324.5, 36.43, new Short("1"))

                        )
        );

        //3.打印
        dataStream.print();

        //4.执行
        env.execute();
    }

}
