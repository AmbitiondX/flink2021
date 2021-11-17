package com.atguigu.day08;

import com.atguigu.day02.source.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.*;

public class Flink16_GroupWindow_Tumble_Count {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
        SingleOutputStreamOperator<WaterSensor> andWatermarks = env.fromElements(
                        new WaterSensor("sensor_1", 1000L, 10),
                        new WaterSensor("sensor_1", 2000L, 20),
                        new WaterSensor("sensor_2", 3000L, 30),
                        new WaterSensor("sensor_1", 5000L, 40),
                        new WaterSensor("sensor_2", 5000L, 40)
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                                .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                                    @Override
                                    public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                                        return element.getTs();
                                    }
                                }));

        // 获取表的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 从流中获取表
        // 用已有的字段作为时间属性
        Table waterSensorTable = tableEnv.fromDataStream(andWatermarks, $("id"), $("ts"), $("vc"), $("pt").proctime());

        /*
            开启一个基于处理时间的翻滚行计数窗口
            1.不能使用时间时间，必须使用过处理时间
            2.当所有条数达不到rowInterval的值时，不会关闭窗口，所以该窗口就不产出结果
         */
        waterSensorTable
                .window(Tumble.over(rowInterval(2L)).on($("pt")).as("w")) // 定义滚动窗口并给窗口起一个别名
                .groupBy($("id"), $("w"))// 窗口字段，必须出现在分组字段中
                .select($("id"), $("vc").sum().as("vc_sum"))
                .execute().print();

    }
}
