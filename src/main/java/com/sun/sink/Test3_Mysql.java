package com.sun.sink;

import com.sun.models.BatteryData;
import com.sun.source.Test4_User_Defined;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * @Author Matt Sun
 * @Date 2021/3/21 1:02 下午
 * @Version 1.0
 **/
public class Test3_Mysql {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

//        DataStream<String> inputStream = env.readTextFile("/Users/mattsun/IdeaProjects/flink-study/src/main/resources/BatteryDate.txt");
//
//        DataStream<BatteryData> dataStream = inputStream.map(new MapFunction<String, BatteryData>() {
//            @Override
//            public BatteryData map(String value) throws Exception {
//                String[] fields = value.split(",");
//                return new BatteryData(fields[0], Long.valueOf(fields[1]), Double.valueOf(fields[2]),
//                        Double.valueOf(fields[3]), Short.valueOf(fields[4]));
//            }
//        });

        DataStream<BatteryData> dataStream = env.addSource(new Test4_User_Defined.BatteryDataGenerator());

        dataStream.addSink(new MyJdbcSink());

        env.execute();

    }

    public static class MyJdbcSink extends RichSinkFunction<BatteryData> {

        Connection connection = null;

        //声明预编译
        PreparedStatement insertStms = null;
        PreparedStatement updateStms = null;

        @Override
        public void open(Configuration parameters) throws Exception {
            connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/test?useSSL=false", "root", "sun123456");


            insertStms = connection.prepareStatement("insert into batterydata (vin,totalVoltage) values(?, ?)");
            updateStms = connection.prepareStatement("update batterydata set totalVoltage = ? where vin = ?");

        }

        @Override
        public void close() throws Exception {
            insertStms.close();
            updateStms.close();
            connection.close();
        }

        //每传来一条数据，调用连接执行sql
         @Override
        public void invoke(BatteryData value, Context context) throws Exception {
            //直接执行更新操作，如果没有成功就插入
             updateStms.setDouble(1, value.getTotalVoltage());
             updateStms.setString(2, value.getVin());

             updateStms.execute();

             if (updateStms.getUpdateCount() == 0){
                 insertStms.setString(1, value.getVin());
                 insertStms.setDouble(2, value.getTotalVoltage());
                 insertStms.execute();
             }

        }
    }

}
