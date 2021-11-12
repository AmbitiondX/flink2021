package com.atguigu.day03.sink_test;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.day02.source.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

public class Flink01_Sink_Kafka {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //2.从端口读取数据
        DataStreamSource<String> source = env.socketTextStream("localhost", 7777);
        SingleOutputStreamOperator<String> jsonStringStream = source.map(new MapFunction<String, String>() {
            @Override
            public String map(String s) throws Exception {
                String[] split = s.split(" ");
                String jsonString = JSONObject.toJSONString(new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2])));
                return jsonString;
            }
        });


        // todo 将数据写到kafka
        jsonStringStream.addSink(new FlinkKafkaProducer<String>("hadoop102:9092","sensor",new SimpleStringSchema()));

        env.execute();
    }
}
