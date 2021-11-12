package com.atguigu.day04.project;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class Flink01_Project_PV_FlatMap {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> source = env.readTextFile("input/UserBehavior.csv");
        source.flatMap(new FlatMapFunction<String, Tuple2<String,Long>>() {
            private Long count = 0L;
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Long>> out) throws Exception {
                String[] split = value.split(",");
                if ("pv".equals(split[3])) {
                    out.collect(Tuple2.of("pv",++count));
                }
            }
        }).print();



        env.execute();
    }
}
