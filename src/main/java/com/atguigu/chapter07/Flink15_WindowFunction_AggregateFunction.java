package com.atguigu.chapter07;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class Flink15_WindowFunction_AggregateFunction {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.从端口获取数据
        DataStreamSource<String> streamSource = env.socketTextStream("localhost", 7777);

        //3.将数据转为Tuple
        SingleOutputStreamOperator<Tuple2<String, Long>> wordToOneDStream = streamSource.flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Long>> out) throws Exception {
                String[] split = value.split(" ");
                for (String s : split) {
                    out.collect(Tuple2.of(s, 1L));
                }
            }
        });

        //4.将相同的单词聚合到同一个分区
        KeyedStream<Tuple2<String, Long>, Tuple> keyedStream = wordToOneDStream.keyBy(0);

        // todo 开启一个基于时间的滑动窗口
        keyedStream
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                        .aggregate(new AggregateFunction<Tuple2<String, Long>, Long, Long>() {
                            @Override
                            public Long createAccumulator() {
                                return 0L;
                            }

                            @Override
                            public Long add(Tuple2<String, Long> value, Long accumulator) {
                                return accumulator + value.f1;
                            }

                            @Override
                            public Long getResult(Long accumulator) {
                                return accumulator;
                            }

                            @Override
                            public Long merge(Long a, Long b) {
                                return a + b;
                            }
                        })
                .print();

        env.execute();
    }
}
