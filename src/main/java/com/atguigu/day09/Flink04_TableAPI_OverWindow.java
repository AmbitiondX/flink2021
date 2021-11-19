package com.atguigu.day09;


import com.atguigu.day02.source.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Over;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.*;

public class Flink04_TableAPI_OverWindow {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //2.获取数据
        SingleOutputStreamOperator<WaterSensor> waterSensorStream = env
                .fromElements(new WaterSensor("sensor_1", 1000L, 10),
                        new WaterSensor("sensor_1", 2000L, 20),
                        new WaterSensor("sensor_2", 3000L, 30),
                        new WaterSensor("sensor_1", 4000L, 40),
                        new WaterSensor("sensor_1", 5000L, 50),
                        new WaterSensor("sensor_2", 6000L, 60))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner((element, recordTimestamp) -> element.getTs())
                );

        //4.将流转为表，并指定事件时间字段
        Table table = tableEnv.fromDataStream(waterSensorStream, $("id"), $("ts").rowtime(), $("vc"));

        //5.查询表中数据，并开启Over窗口
        // todo 使用over开窗时需要注意，必须指定orderby，对于流处理，orderby字段必须时时间字段
        /*
        //窗口范围：上无边界到当前行
        table
                .window(Over.partitionBy($("id")).orderBy($("ts")).as("w"))
                .select($("id"),$("ts"),$("vc"),$("vc").sum().over($("w")))
                .execute()
                .print();
         */

       /* //窗口范围：往前推算两行
        table
                .window(Over.partitionBy($("id")).orderBy($("ts")).preceding(rowInterval(2L)).as("w"))
                .select($("id"),$("ts"),$("vc"),$("vc").sum().over($("w")))
                .execute()
                .print();
       */

         //窗口范围：前推算两秒
        table
                .window(Over.partitionBy($("id")).orderBy($("ts")).preceding(lit(2).second()).as("w"))
                .select($("id"),$("ts"),$("vc"),$("vc").sum().over($("w")))
                .execute()
                .print();

    }
}
