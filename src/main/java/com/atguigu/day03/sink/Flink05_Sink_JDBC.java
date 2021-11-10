package com.atguigu.day03.sink;

import com.atguigu.day02.source.WaterSensor;
import com.mysql.jdbc.Driver;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcOutputFormat;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.internal.GenericJdbcSinkFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink05_Sink_JDBC {
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

        /*
            <dependency>
                <groupId>org.apache.flink</groupId>
                <artifactId>flink-connector-jdbc_2.11</artifactId>
                <version>1.12.3</version>
            </dependency>
         */
        waterSensorDStream.addSink(JdbcSink.sink(
                "insert into sensor values(?,?,?)",
                (ps,t) -> {
                  ps.setString(1,t.getId());
                  ps.setLong(2,t.getTs());
                  ps.setInt(3,t.getVc());
                },
                // 设置批次大小，默认值：5000
                new JdbcExecutionOptions.Builder().withBatchSize(1).build(),
                new JdbcConnectionOptions
                        .JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:mysql://localhost:3306/db1?useSSL=false")
                        .withPassword("root")
                        .withUsername("root")
                        .withDriverName(Driver.class.getName())
                        .build()
        ));

        env.execute();
    }
}
