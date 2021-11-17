package com.atguigu.day08;

import com.atguigu.day02.source.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class Flink08_SQLApi_Kafka2Kafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        // 获取表的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 注册sourceTable：source_sensor
        tableEnv.executeSql(
                "create table source_sensor (id string,ts bigint, vc int) with ("
                + "'connector' = 'kafka',"
                + "'topic' = 'topic_source_sensor',"
                + "'properties.bootstrap.servers' = 'hadoop102:9092',"
                + "'properties.group.id' = 'atguigu',"
                + "'format' = 'csv')"
        );

        // 注册sinkTable：sink_sensor
        tableEnv.executeSql(
                "create table sink_sensor (id string,ts bigint, vc int) with ("
                + "'connector' = 'kafka',"
                + "'topic' = 'topic_sink_sensor',"
                + "'properties.bootstrap.servers' = 'hadoop102:9092',"
                + "'format' = 'csv')"
        );

        //  从SourceTable 查询数据, 并写入到 SinkTable
        tableEnv.executeSql("insert into sink_sensor select id,ts,vc from source_sensor where id = 'sensor_1'");


    }
}
