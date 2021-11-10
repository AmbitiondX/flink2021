package com.atguigu.day03.transform;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class Flink03_TransForm_Union_socket {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> source1 = env.socketTextStream("hadoop102", 7777);
        DataStreamSource<String> source2 = env.socketTextStream("hadoop102", 8888);
        DataStreamSource<String> source3 = env.socketTextStream("hadoop102", 9999);

        //TODO 利用Union连接两条流 水乳交融
        DataStream<String> union = source1.union(source2, source3);

        union.process(new ProcessFunction<String, String>() {
            @Override
            public void processElement(String value, ProcessFunction<String, String>.Context ctx, Collector<String> out) throws Exception {
                out.collect(value);
            }
        }).print();
        
        env.execute();
    }
}
