package com.atguigu.day08;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Flink12_SQLApi_Eventtime02 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        // 获取表的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // 当发现时区所导致的时间问题时，可设置本地使用的时区：
        Configuration configuration = tableEnv.getConfig().getConfiguration();
        // GMT - 格林威治时间
        configuration.setString("table.local-time-zone", "GMT");

        // 作为事件时间的字段必须是 timestamp(3) 类型, 所以根据 long 类型的 ts 计算出来一个 t
        tableEnv.executeSql("create table sensor (" +
                "id string, " +
                "ts bigint, " +
                "vc int, " +
                "t as to_timestamp(from_unixtime(ts/1000,'yyyy-MM-dd HH:mm:ss'))," +
                "watermark for t as t - interval '5' second) " +
                "with ("
                        + " 'connector' = 'filesystem',"
                        + " 'path' = 'input/sensor-sql.txt',"
                        + " 'format' = 'csv')" );

        tableEnv.executeSql("select * from sensor").print();

        /*
            说明：
            1.把一个现有的列定义为一个为表标记事件时间的属性。该列的类型必须为TIMESTAMP(3)，且是 schema 中的顶层列，它也可以是一个计算列。
            2.严格递增时间戳：WATERMARK FOR rowtime_column AS rowtime_column。
            3.递增时间戳：WATERMARK FOR rowtime_column AS rowtime_column - INTERVAL '0.001' SECOND。
            4.有界乱序时间戳： WATERMARK FOR rowtime_column AS rowtime_column - INTERVAL 'string' timeUnit。
         */
    }
}
