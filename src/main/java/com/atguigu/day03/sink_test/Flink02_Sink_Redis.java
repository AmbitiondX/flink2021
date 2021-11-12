package com.atguigu.day03.sink_test;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.day02.source.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

public class Flink02_Sink_Redis {
    public static void main(String[] args) throws Exception {
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

        /*
            <dependency>
                <groupId>org.apache.bahir</groupId>
                <artifactId>flink-connector-redis_2.11</artifactId>
                <version>1.0</version>
            </dependency>
         */

        // todo 将数据写到redis（hash）
        FlinkJedisPoolConfig poolConfig = new FlinkJedisPoolConfig.Builder().setHost("hadoop102").setPort(6379).build();
        jsonStringStream.addSink(new RedisSink<>(poolConfig, new RedisMapper<String>() {
            @Override
            public RedisCommandDescription getCommandDescription() {
                return new RedisCommandDescription(RedisCommand.HSET,"0625");
            }

            @Override
            public String getKeyFromData(String s) {
                String id = JSONObject.parseObject(s).getString("id");
                return id;
            }

            @Override
            public String getValueFromData(String s) {
                return s;
            }
        }));

        // todo 将数据写到redis（set）


        env.execute();
    }
}
