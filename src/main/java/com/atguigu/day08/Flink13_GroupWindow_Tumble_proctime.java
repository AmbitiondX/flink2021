package com.atguigu.day08;

import com.atguigu.day02.source.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.lit;

public class Flink13_GroupWindow_Tumble_proctime {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
        SingleOutputStreamOperator<WaterSensor> waterSensorDataStreamSource = env
                .socketTextStream("localhost", 7777)
                .map(line -> {
                    String[] split = line.split(" ");
                    return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
                });

        // 获取表的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 从流中获取表
        // 用已有的字段作为时间属性
        Table waterSensorTable = tableEnv.fromDataStream(waterSensorDataStreamSource, $("id"), $("ts"), $("vc"),$("pt").proctime());

        // todo 基于处理时间的开窗，必须是无界流，才能正确产出数据，有界流不打印结果
        waterSensorTable
                .window(Tumble.over(lit(4).second()).on($("pt")).as("w")) // 定义滚动窗口并给窗口起一个别名
                .groupBy($("id"),$("w"))// 窗口字段，必须出现在分组字段中
                .select($("id"),$("vc").sum().as("vc_sum"),$("w").start(),$("w").end())
                .execute().print();

    }
}
