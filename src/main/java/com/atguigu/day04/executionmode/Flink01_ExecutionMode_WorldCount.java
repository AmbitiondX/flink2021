package com.atguigu.day04.executionmode;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class Flink01_ExecutionMode_WorldCount {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setRuntimeMode(RuntimeExecutionMode.BATCH);
        env.setParallelism(1);

        env
                .readTextFile("input/word.txt")
                .flatMap(new FlatMapFunction<String, Tuple2<String,Integer>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String,Integer>> out) throws Exception {
                        String[] split = value.split(" ");
                        for (String s : split) {
                            out.collect(Tuple2.of(s,1));
                        }
                    }
                })
                .keyBy(0)
                .sum(1)
                .print();

        env.execute();
    }
}
