package com.atguigu.day03.sink;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.day02.source.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

public class Flink04_Sink_Custom_Mysql {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.从端口读取数据
        SingleOutputStreamOperator<WaterSensor> waterSensorDStream = env.socketTextStream("hadoop102", 7777)
                .map(new MapFunction<String, WaterSensor>() {
                    @Override
                    public WaterSensor map(String value) throws Exception {
                        String[] split = value.split(" ");
                        return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
                    }
                });

        /*
            <dependency>
                 <groupId>mysql</groupId>
                 <artifactId>mysql-connector-java</artifactId>
                 <version>5.1.49</version>
            </dependency>
         */
        waterSensorDStream.addSink(new MySink());

        env.execute();
    }

    public static class MySink extends RichSinkFunction<WaterSensor> {
        private Connection connection = null;
        private PreparedStatement pstm = null;

        @Override
        public void invoke(WaterSensor value, Context context) throws Exception {
            pstm.setString(1,value.getId());
            pstm.setLong(2,value.getTs());
            pstm.setInt(3,value.getVc());
            pstm.execute();
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            // 最好在创建之前判断是否为null
            connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/db1?useSSL=false", "root", "root");
            pstm = connection.prepareStatement("insert into sensor values(?,?,?)");
        }

        @Override
        public void close() throws Exception {
            // 最好在关闭之前判断是否为null
            pstm.close();
            connection.close();
        }
    }
}
