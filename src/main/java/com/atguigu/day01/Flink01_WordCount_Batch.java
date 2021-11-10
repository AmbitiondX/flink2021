package com.atguigu.day01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class Flink01_WordCount_Batch {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<String> dataSource = env.readTextFile("input/word.txt");
        FlatMapOperator<String, String> flatMap = dataSource.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String s, Collector<String> collector) throws Exception {
                String[] split = s.split(" ");
                for (String s1 : split) {
                    collector.collect(s1);
                }
            }
        });

        // 使用map组成元组
        MapOperator<String, Tuple2<String, Integer>> map = flatMap.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2 map(String s) throws Exception {
                return Tuple2.of(s, 1);
            }
        });
        map.print();

        UnsortedGrouping<Tuple2<String, Integer>> groupBy = map.groupBy(0);

        AggregateOperator<Tuple2<String, Integer>> sum = groupBy.sum(1);

//        sum.print();


    }
}
