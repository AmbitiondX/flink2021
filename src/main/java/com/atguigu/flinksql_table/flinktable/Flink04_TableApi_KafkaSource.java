package com.atguigu.flinksql_table.flinktable;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.*;
import org.apache.flink.types.Row;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

import static org.apache.flink.table.api.Expressions.$;

public class Flink04_TableApi_KafkaSource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1. 创建表的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 2. 创建表
        // 2.1 表的元数据信息
        Schema schema = new Schema()
                .field("id", DataTypes.STRING())
                .field("ts", DataTypes.BIGINT())
                .field("vc", DataTypes.INT());

        // 2.2 连接文件, 并创建一个临时表, 其实就是一个动态表
        Properties properties = new Properties();
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "hadoop102:9092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "flinkTalbe");
        tableEnv.connect(new Kafka()
                        .properties(properties)
                        .topic("sensor")
                        .startFromLatest())
                .withFormat(new Json())
                .withSchema(schema)
                .createTemporaryTable("sensor");

        // 3. 做成表对象，然后对动态表进行查询
        Table sensorTable = tableEnv.from("sensor");
        Table resultTable = sensorTable
                .groupBy($("id"))
                .select($("id"), $("id").count().as("cnt"));

        // 4. 把动态表转换成流，如果涉及到数据的更新，要用到撤回流，多了一个boolean标记
        DataStream<Tuple2<Boolean, Row>> resultStream = tableEnv.toRetractStream(resultTable, Row.class);

        resultStream.print();

        env.execute();
    }
}
