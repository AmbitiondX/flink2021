package com.atguigu.day04.project;

import org.apache.flink.api.common.functions.MapFunction;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


public class Flink06_Project_Ads_Click_01 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .readTextFile("input/AdClickLog.csv")
                .map(new MapFunction<String, Tuple2<Tuple2<String,Long>,Long>>() {
                    @Override
                    public Tuple2<Tuple2<String, Long>, Long> map(String value) throws Exception {
                        String[] split = value.split(",");
                        return Tuple2.of(Tuple2.of(split[2],Long.parseLong(split[1])),1L);
                    }
                })
                .keyBy(0)
                .sum(1)
                .print("省份-广告");

        env.execute();
    }
}
