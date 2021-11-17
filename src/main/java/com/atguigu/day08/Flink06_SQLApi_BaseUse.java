package com.atguigu.day08;

import com.atguigu.day02.source.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

public class Flink06_SQLApi_BaseUse {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
        DataStreamSource<WaterSensor> waterSensorDataStreamSource = env.fromElements(
                new WaterSensor("sensor_1", 1000L, 10),
                new WaterSensor("sensor_1", 1000L, 20),
                new WaterSensor("sensor_2", 1000L, 30),
                new WaterSensor("sensor_1", 1000L, 40),
                new WaterSensor("sensor_2", 1000L, 40)
        );

        // 获取表的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 从流中获取表
        Table waterSensorTable = tableEnv.fromDataStream(waterSensorDataStreamSource);

        // 查询表
        Table resultTable = tableEnv.sqlQuery("select * from " + waterSensorTable + " where vc >= 20");

        // 将表追加到流当中，打印出来
        tableEnv.toAppendStream(resultTable, Row.class).print();

        env.execute();
    }
}
