package com.atguigu.day03.sink;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.day02.source.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
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
                <groupId>org.apache.bahir</groupId>
                <artifactId>flink-connector-redis_2.11</artifactId>
                <version>1.0</version>
            </dependency>
         */

        // todo 将数据写到redis（hash）
        /*FlinkJedisPoolConfig jedisPool = new FlinkJedisPoolConfig.Builder().setHost("hadoop102").setPort(6379).build();
        map.addSink(new RedisSink<>(jedisPool, new RedisMapper<String>() {
            @Override
            public RedisCommandDescription getCommandDescription() {
                return new RedisCommandDescription(RedisCommand.HSET,"0625");
            }

            @Override
            public String getKeyFromData(String data) {
                WaterSensor waterSensor = JSONObject.parseObject(data, WaterSensor.class);
                return waterSensor.getId();
            }

            @Override
            public String getValueFromData(String data) {
                return data;
            }
        }));*/

        // todo 将数据写到redis（set）
        FlinkJedisPoolConfig poolConfig = new FlinkJedisPoolConfig.Builder().setHost("hadoop102").build();
        map.addSink(new RedisSink<>(poolConfig, new RedisMapper<String>() {
            @Override
            public RedisCommandDescription getCommandDescription() {
                return new RedisCommandDescription(RedisCommand.SADD);
            }

            @Override
            public String getKeyFromData(String data) {
                JSONObject jsonObject = JSONObject.parseObject(data);
                return jsonObject.getString("id");
            }

            @Override
            public String getValueFromData(String data) {
                return data;
            }
        }));

        env.execute();
    }
}
