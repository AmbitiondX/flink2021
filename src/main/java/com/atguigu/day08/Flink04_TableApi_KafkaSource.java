package com.atguigu.day08;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.*;
import org.apache.flink.types.Row;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import static org.apache.flink.table.api.Expressions.$;

public class Flink04_TableApi_KafkaSource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        // 获取表的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        Schema schema = new Schema()
                .field("id", DataTypes.STRING())
                .field("ts", DataTypes.BIGINT())
                .field("vc", DataTypes.INT());

        // 连接文件 创建一张临时表(动态表)
        tableEnv.connect(
                new Kafka()
                        .topic("sensor")
                        .version("universal")
                        .property(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092")
                        .property(ConsumerConfig.GROUP_ID_CONFIG, "flinkTable")
                )
                .withFormat(new Json())
                .withSchema(schema)
                .createTemporaryTable("sensor");

        // 将临时表做成表对象，然后对动态表进行查询
        Table sensorTable = tableEnv.from("sensor");
        Table resultTable = sensorTable
                .where($("vc").isGreaterOrEqual(20))
                .groupBy($("id"))
                .select($("id"), $("vc").sum());



        // 将表追加到流当中，打印出来
        tableEnv.toRetractStream(resultTable, Row.class).print();

        env.execute();
    }
}
