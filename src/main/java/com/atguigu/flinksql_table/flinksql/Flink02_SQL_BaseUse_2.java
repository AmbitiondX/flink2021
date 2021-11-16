package com.atguigu.flinksql_table.flinksql;

import com.atguigu.day02.source.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class Flink02_SQL_BaseUse_2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
        DataStreamSource<WaterSensor> waterSensorStream =
                env.fromElements(new WaterSensor("sensor_1", 1000L, 10),
                        new WaterSensor("sensor_1", 2000L, 20),
                        new WaterSensor("sensor_2", 3000L, 30),
                        new WaterSensor("sensor_1", 4000L, 40),
                        new WaterSensor("sensor_1", 5000L, 50),
                        new WaterSensor("sensor_2", 6000L, 60));

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // todo 使用sql查询一个一注册的表
        // 从流中获取一张表
        Table inputTable = tableEnv.fromDataStream(waterSensorStream);

        // 把表注册成一张临时视图
        tableEnv.createTemporaryView("sensor",inputTable);

        // 在临时视图中查询数据，获取一张新的表
        Table resultTable = tableEnv.sqlQuery("select * from sensor where id = 'sensor_1'");

        // 现实resultTable中的数据
        tableEnv.toAppendStream(resultTable, Row.class).print();

        env.execute();


    }
}
