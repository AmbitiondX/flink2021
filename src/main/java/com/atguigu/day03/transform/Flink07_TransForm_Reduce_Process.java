package com.atguigu.day03.transform;

import com.atguigu.day02.source.WaterSensor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class Flink07_TransForm_Reduce_Process {
    public static void main(String[] args) throws Exception {
        // 获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从端口读取数据
        DataStreamSource<String> source = env.socketTextStream("hadoop102", 7777);

        // todo 使用process实现Map方法
        SingleOutputStreamOperator<WaterSensor> process = source.process(new ProcessFunction<String, WaterSensor>() {
            @Override
            public void processElement(String value, ProcessFunction<String, WaterSensor>.Context ctx, Collector<WaterSensor> out) throws Exception {
                String[] split = value.split(" ");
                out.collect(new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2])));
            }
        });
        process.print();

        // todo 利用process实现累加器功能
        KeyedStream<WaterSensor, Tuple> keyBy = process.keyBy("id");


        SingleOutputStreamOperator<Integer> process1 = keyBy.process(new KeyedProcessFunction<Tuple, WaterSensor, Integer>() {
            private Integer count = 0;

            @Override
            public void processElement(WaterSensor value, KeyedProcessFunction<Tuple, WaterSensor, Integer>.Context ctx, Collector<Integer> out) throws Exception {
                System.out.println("process...");
                count++;
                out.collect(count);
            }
        });

        process1.print();


        env.execute();
    }
}
