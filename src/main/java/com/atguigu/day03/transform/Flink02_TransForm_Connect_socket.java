package com.atguigu.day03.transform;

import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;


public class Flink02_TransForm_Connect_socket {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

//        DataStreamSource<String> source1 = env.fromElements("a", "b", "c", "d", "e");
//        DataStreamSource<Integer> source2 = env.fromElements(1, 2, 3, 4, 5);

        DataStreamSource<String> source1 = env.socketTextStream("hadoop102", 7777);
        DataStreamSource<String> source2 = env.socketTextStream("hadoop102", 8888);


        ConnectedStreams<String, String> connect = source1.connect(source2);

        connect.process(new CoProcessFunction<String, String, String>() {
            @Override
            public void processElement1(String value, CoProcessFunction<String, String, String>.Context ctx, Collector<String> out) throws Exception {
                out.collect(value);
            }

            @Override
            public void processElement2(String value, CoProcessFunction<String, String, String>.Context ctx, Collector<String> out) throws Exception {
                out.collect(value);
            }
        }).print();


        env.execute();
    }
}
