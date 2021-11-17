package com.atguigu.day08;

import com.atguigu.day02.source.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

public class Flink10_SQLApi_Proctime02 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        // 获取表的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.executeSql("create table sensor (id string, ts bigint, vc int, pt_time as PROCTIME()) with ("
                        + " 'connector' = 'filesystem',"
                        + " 'path' = 'input/sensor.txt',"
                        + " 'format' = 'csv')" );

        tableEnv.executeSql("select * from sensor").print();

//        env.execute();
    }
}
