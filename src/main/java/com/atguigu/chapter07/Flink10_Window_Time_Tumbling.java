package com.atguigu.chapter07;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class Flink10_Window_Time_Tumbling {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> streamSource = env.socketTextStream("localhost", 7777);
        SingleOutputStreamOperator<Tuple2<String, Long>> flatMap = streamSource.flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Long>> out) throws Exception {
                String[] split = value.split(" ");
                for (String s : split) {
                    out.collect(Tuple2.of(s, 1L));
                }
            }
        });

        // 将相同的单词聚合到同一个分区
        KeyedStream<Tuple2<String, Long>, Tuple> keyBy = flatMap.keyBy(0);

        // todo 开启一个基于时间的滚动窗口
        WindowedStream<Tuple2<String, Long>, Tuple, TimeWindow> window = keyBy.window(TumblingProcessingTimeWindows.of(Time.seconds(10)));

        SingleOutputStreamOperator<String> process = window.process(new ProcessWindowFunction<Tuple2<String, Long>, String, Tuple, TimeWindow>() {
            @Override
            public void process(Tuple tuple, ProcessWindowFunction<Tuple2<String, Long>, String, Tuple, TimeWindow>.Context context, Iterable<Tuple2<String, Long>> elements, Collector<String> out) throws Exception {
                String msg = "窗口:[" + context.window().getStart() / 1000 + "," + context.window().getEnd() / 1000 + ") 一共有"
                        + elements.spliterator().estimateSize() + "条数据";
                out.collect(msg);
            }
        });

        process.print();
        window.sum(1).print();
        env.execute();
    }
}
