package com.sun.table;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

/**
 * @Author Matt Sun
 * @Date 2021/3/28 1:26 下午
 * @Version 1.0
 **/
public class Test2_CommonAPI {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //1.1 基于老版本的planner流处理
        EnvironmentSettings oldStreamSettings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .useOldPlanner()
                .build();

        StreamTableEnvironment oldStreamTableEnv = StreamTableEnvironment.create(env, oldStreamSettings);

        //1.2 基于老版本的planner的批处理
        ExecutionEnvironment batchEnv = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment batchTableEnv = BatchTableEnvironment.create(batchEnv);

        //1.3 基于blink的流处理
        EnvironmentSettings blinkStreamSettings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .useBlinkPlanner()
                .build();

        StreamTableEnvironment blinkStreamTableEnv = StreamTableEnvironment.create(env, blinkStreamSettings);

        //1.4 基于blink的批处理
        EnvironmentSettings blinkBatchSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inBatchMode()
                .build();
        TableEnvironment blinkBatchTableEnv = TableEnvironment.create(blinkBatchSettings);

        // 2.表的创建，连接外部系统，读取数据
        // 2.1 读取文件
        String filePath = "/Users/mattsun/IdeaProjects/flink-study/src/main/resources/BatteryDate.txt";
        tableEnv.connect(new FileSystem().path(filePath))
                .withFormat(new Csv())
                .withSchema(new Schema()
                .field("vin", DataTypes.STRING())
                .field("timestamp", DataTypes.BIGINT())
                .field("totalVoltage", DataTypes.DOUBLE())
                .field("totalCurrent", DataTypes.DOUBLE())
                .field("chrgSts", DataTypes.SMALLINT()))
                .createTemporaryTable("inputTable");

        Table inputTable = tableEnv.from("inputTable");
//        inputTable.printSchema();
//        tableEnv.toAppendStream(inputTable, Row.class).print("inputTable");

        //3. 查询转换
        //3.1 table API
        //简单转换
        Table filterTable = inputTable.select("vin, totalVoltage")
                .filter("vin = 'LGWWCM003'");
        //聚合统计
        Table avgVoltageTable = inputTable.groupBy("vin")
                .select("vin, vin.count as count, totalVoltage.avg as avgVoltage");

//        tableEnv.toAppendStream(filterTable, Row.class).print("filterTable");
//        tableEnv.toRetractStream(avgVoltageTable, Row.class).print("avgVoltageTable");

        //3.2 sql
        Table sqlQuery = tableEnv.sqlQuery("select vin, totalVoltage from inputTable where vin = 'LGWWCM003'");
        Table sqlQuery1 = tableEnv.sqlQuery("select vin, count(vin) as cnt , avg(totalVoltage) as avgVolatage from inputTable group by vin");

        tableEnv.toAppendStream(sqlQuery, Row.class).print("sql1");
        tableEnv.toRetractStream(sqlQuery1, Row.class).print("sql2");


        env.execute();


    }

}
