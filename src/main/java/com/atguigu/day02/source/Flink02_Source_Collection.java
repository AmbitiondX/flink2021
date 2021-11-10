package com.atguigu.day02.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.NumberSequenceIterator;

import java.util.Arrays;
import java.util.List;

public class Flink02_Source_Collection {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(2);

        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6, 7);
        DataStreamSource<Integer> source = env.fromCollection(list);

        //  报错：The parallelism of non parallel operator must be 1.
        DataStreamSource<Integer> source1 = env.fromCollection(list).setParallelism(2);
        //多并行度方法，核心是实现了ParallelSourceFunction这个接口
//        env.fromParallelCollection()
        source.print();
        env.execute();
    }
}
