package com.atguigu.flinksql_table.window;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Flink03_SQLApi_GroupWindows {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 作为事件时间的字段必须是timestamp类型，所以根据long类型的ts计算出一个t
        tableEnv.executeSql("create table sensor (" +
                "id string," +
                "ts bigint," +
                "vc int," +
                "t as to_timestamp(from_unixtime(ts/1000,'yyyy-MM-dd HH:mm:ss'))," +
                "watermark for t as t - interval '5' second)" +
                "with(" +
                "'connector' = 'filesystem'," +
                "'path' = 'input/sensor-sql.txt'," +
                "'format' = 'csv'" +
                ")");

        tableEnv.executeSql(
                "SELECT id, " +
                        "  TUMBLE_START(t, INTERVAL '2' second) as wStart,  " +
                        "  TUMBLE_END(t, INTERVAL '2' second) as wEnd,  " +
                        "  SUM(vc) sum_vc " +
                        "FROM sensor " +
                        "GROUP BY TUMBLE(t, INTERVAL '2' second), id"
        )
                .print();

        tableEnv.executeSql(
                "SELECT id, " +
                        "  hop_start(t, INTERVAL '1' second, INTERVAL '1' second) as wStart,  " +
                        "  hop_end(t, INTERVAL '1' second, INTERVAL '1' second) as wEnd,  " +
                        "  SUM(vc) sum_vc " +
                        "FROM sensor " +
                        "GROUP BY hop(t, INTERVAL '1' second, INTERVAL '1' second), id"
        )
                .print();
    }
}
