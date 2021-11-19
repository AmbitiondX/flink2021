package com.atguigu.day09;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Flink01_SQL_GroupWindow_Tumbling {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // t AS to_timestamp(from_unixtime(ts/1000,'yyyy-MM-dd HH:mm:ss'))
        // t AS 整体
        tableEnv.executeSql("CREATE TABLE sensor (\n" +
                "    id STRING,\n" +
                "    ts BIGINT,\n" +
                "    vc INT,\n" +
                "    t AS  to_timestamp(from_unixtime(ts/1000,'yyyy-MM-dd HH:mm:ss')),\n" +
                "    WATERMARK FOR t AS t - INTERVAL '5' SECOND\n" +
                ") WITH ( " +
                "    'connector' = 'filesystem',\n" +
                "    'path' = 'input/sensor-sql.txt',\n" +
                "    'format' = 'csv'" +
                ")");

        // todo 开启一个基于时间时间的滚动窗口
        // todo 当读取有界数据时，使用处理时间(文件太小，读取速度太快)，导窗口无法关闭，所以没有数据输出
        // 处理时间读取有界流，按照时间开窗没有意义，因为读取速度太快，把握不好矿窗口大小，而且还可能丢数据
        tableEnv.executeSql(
                "select id,\n" +
                        "TUMBLE_START(t, interval '3' second),\n" +
                        "TUMBLE_END(t, interval '3' second),\n" +
                        "sum(vc) \n" +
                        "from sensor \n " +
                        "group by TUMBLE(t, interval '3' second), id"
        ).print();
    }
}
