package com.atguigu.day04.project;

import com.atguigu.day04.project.bean.UserBehavior;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashSet;

public class Flink02_Project_UV_Process {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> source = env.readTextFile("input/UserBehavior.csv");
        source.process(new ProcessFunction<String, Tuple2<String,Long>>() {
            HashSet<String> set = new HashSet<>();
            @Override
            public void processElement(String value, ProcessFunction<String, Tuple2<String, Long>>.Context ctx, Collector<Tuple2<String, Long>> out) throws Exception {
                String[] split = value.split(",");
                if ("pv".equals(split[3])){
                    set.add(split[0]);
                    out.collect(Tuple2.of("uv",(long)set.size()));
                }
            }
        }).print();


        env.execute();
    }
}
