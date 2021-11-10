package com.atguigu.day03.sink;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.day02.source.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

public class Flink01_Sink_Kafka {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //2.从端口读取数据
        SingleOutputStreamOperator<WaterSensor> waterSensorDStream = env.socketTextStream("hadoop102", 7777)
                .map(new MapFunction<String, WaterSensor>() {
                    @Override
                    public WaterSensor map(String value) throws Exception {
                        String[] split = value.split(" ");
                        return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
                    }
                });
        SingleOutputStreamOperator<String> map = waterSensorDStream.map(new MapFunction<WaterSensor, String>() {
            @Override
            public String map(WaterSensor value) throws Exception {
                String jsonString = JSONObject.toJSONString(value);
                return jsonString;
            }
        });

        /*
            <dependency>
                <groupId>org.apache.flink</groupId>
                <artifactId>flink-connector-kafka_2.11</artifactId>
                <version>1.12.3</version>
            </dependency>
         */
        // todo 将数据写到kafka
        map.addSink(new FlinkKafkaProducer<String>("hadoop103:9092","topic_sensor",new SimpleStringSchema()));
//        Properties properties = new Properties();
//        properties.setProperty("bootstrap.servers", "hadoop103:9092");
//        map.addSink(new FlinkKafkaProducer<String>("topic_sensor", new SimpleStringSchema(), properties));

        env.execute();
    }
}
