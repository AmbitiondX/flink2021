package com.atguigu.day09;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Flink02_SQL_GroupWindow_Hop {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.executeSql(
                "create table sensor (\n" +
                        "id string,\n" +
                        "ts bigint,\n" +
                        "vc int,\n" +
                        "t as to_timestamp(from_unixtime(ts/1000,'yyyy-MM-dd HH:mm:ss')),\n" +
                        "watermark for t as t - interval '5' second\n" +
                        ")\n" +
                        "with (\n" +
                        " 'connector' = 'filesystem',\n" +
                        " 'path' = 'input/sensor-sql.txt',\n" +
                        " 'format' = 'csv'\n" +
                        ")"
        );

        // todo 开启一个基于时间时间的滚动窗口
        // todo 当读取有界数据时，使用处理时间(文件太小，读取速度太快)，导窗口无法关闭，所以没有数据输出
        tableEnv.executeSql(
                "select \n" +
                        "id,\n" +
                        "HOP_START(t, INTERVAL '2' second, INTERVAL '3' second),\n" +
                        "HOP_END(t, INTERVAL '2' second, INTERVAL '3' second),\n" +
                        "sum(vc) as vc_time\n" +
                        "from sensor\n" +
                        "group by \n" +
                        "id,\n" +
                        "HOP(t, INTERVAL '2' second, INTERVAL '3' second)"
        ).print();
    }
}
