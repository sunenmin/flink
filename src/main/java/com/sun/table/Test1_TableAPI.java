package com.sun.table;

import com.sun.models.BatteryData;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @Author Matt Sun
 * @Date 2021/3/28 11:03 上午
 * @Version 1.0
 **/
public class Test1_TableAPI {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        DataStream<String> inputStream = env.readTextFile("/Users/mattsun/IdeaProjects/flink-study/src/main/resources/BatteryDate.txt");
        DataStream<BatteryData> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new BatteryData(fields[0], Long.valueOf(fields[1]), Double.valueOf(fields[2]),
                    Double.valueOf(fields[3]), Short.valueOf(fields[4]));
        });


        //创建表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //基于数据流创建一张表
        Table dataTable = tableEnv.fromDataStream(dataStream);

        //调用table api进行查询
        Table resultTable = dataTable.select("vin, totalVoltage")
                .where("vin = 'LGWWCM003'");

        //基于sql进行查询
        tableEnv.createTemporaryView("batteryData", dataTable);
        String sql = "select vin,totalVoltage from batteryData where vin='LGWWCM003'";
        Table resultSqlTable = tableEnv.sqlQuery(sql);

        tableEnv.toAppendStream(resultTable, Row.class).print("result:");
        tableEnv.toAppendStream(resultSqlTable, Row.class).print("sql:");

        env.execute();

    }
}
