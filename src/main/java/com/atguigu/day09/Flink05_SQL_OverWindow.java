package com.atguigu.day09;


import com.atguigu.day02.source.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Over;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.lit;

public class Flink05_SQL_OverWindow {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //2.使用sql创建表
        tableEnv.executeSql(
                "create table sensor (\n" +
                        "id string,\n" +
                        "ts bigint,\n" +
                        "vc int,\n" +
                        "t as to_timestamp(from_unixtime(ts/1000,'yyyy-MM-dd HH:mm:ss')),\n" +
                        "watermark for t as t - interval '5' second\n" +
                        ")\n" +
                        "with\n" +
                        "(\n" +
                        "'connector' = 'filesystem',\n" +
                        "'path' = 'input/sensor-sql.txt',\n" +
                        "'format' = 'csv'\n" +
                        ")"
        );

        // todo 在sql中使用overwindow
        // 写法1：
        /*tableEnv.executeSql(
                "select\n" +
                        "id,\n" +
                        "ts,\n" +
                        "vc,\n" +
                        "sum(vc) over(partition by id order by t)\n" +
                        "from sensor\n"
        ).print();
*/
        // 写法2：
        // todo 还不支持 OVER RANGE FOLLOWING 窗口 OVER RANGE FOLLOWING windows are not supported yet .
        tableEnv.executeSql(
                "select\n" +
                        "id,\n" +
                        "ts,\n" +
                        "vc,\n" +
                        "sum(vc) over w,\n" +
                        "count(vc) over w\n" +
                        "from sensor\n" +
                        "window w as (partition by id order by t rows between unbounded preceding and current row )"
        ).print();




    }
}
