package com.atguigu.day02.source;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

public class Flink04_Source_Kafka_keyby {
    public static void main(String[] args) throws Exception {
        // 获取执行环境
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        // 设置并行度为1
        env.setParallelism(2);

        Properties properties = new Properties();
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,"flink-test");
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092");

        // 添加source源
        DataStreamSource<String> source = env.addSource(new FlinkKafkaConsumer<String>("sensor", new SimpleStringSchema(), properties)).setParallelism(2);

        SingleOutputStreamOperator<Tuple2<String, String>> map = source.map(new MapFunction<String, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> map(String value) throws Exception {

                return Tuple2.of("123",value);
            }
        });

        KeyedStream<Tuple2<String, String>, Tuple> keyBy = map.keyBy(0);

        keyBy.print().setParallelism(1);

        env.execute();

    }
}
