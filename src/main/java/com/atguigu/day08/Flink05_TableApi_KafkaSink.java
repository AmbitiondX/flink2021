package com.atguigu.day08;

import com.atguigu.day02.source.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.*;
import org.apache.kafka.clients.producer.ProducerConfig;

import static org.apache.flink.table.api.Expressions.$;

public class Flink05_TableApi_KafkaSink {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
        DataStreamSource<WaterSensor> waterSensorDataStreamSource = env.fromElements(
                new WaterSensor("sensor_1", 1000L, 10),
                new WaterSensor("sensor_1", 2000L, 20),
                new WaterSensor("sensor_2", 3000L, 30),
                new WaterSensor("sensor_1", 4000L, 40),
                new WaterSensor("sensor_1", 5000L, 50),
                new WaterSensor("sensor_2", 6000L, 60)
        );

        // 获取表的执行环境,并从流中获取表
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        Table sensorTable = tableEnv.fromDataStream(waterSensorDataStreamSource);

        // 查询表，产生新表
        Table resultTable = sensorTable
                .where($("vc").isGreaterOrEqual(20))
                .select($("id"), $("ts"), $("vc"));

        // 创建临时视图写入kafka
        tableEnv.connect(
                new Kafka()
                        .topic("sensor")
                        .startFromLatest()
                        .version("universal")
                        .property(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092")
        )
                .withFormat(new Json())
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("ts", DataTypes.BIGINT())
                        .field("vc", DataTypes.INT()))
                .createTemporaryTable("kafkaSensor");


        // 数据写入到输出表中
        resultTable.executeInsert("kafka2Sensor");


    }
}
