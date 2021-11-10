package com.atguigu.day02.transform;

import com.atguigu.day02.source.WaterSensor;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;
import java.util.List;

public class Flink02_Transform_RichMap {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(1);

        // 从端口获取数据
//        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 7777);
//        DataStreamSource<String> streamSource = env.readTextFile("input/word.txt");
        List<String> list = Arrays.asList("1", "2", "3", "4", "5");
        DataStreamSource<String> streamSource = env.fromCollection(list);

        SingleOutputStreamOperator<String> map = streamSource.map(new RichMapFunction<String, String>() {
            /**
             * 在程序结束的时候调用
             * 每个并行度调用一次（当读文件时，每个并行度调用两次,内容读完一次，文件读完一次）
             * 生命周期结束
             * @throws Exception
             */
            @Override
            public void close() throws Exception {
                System.out.println("close...");
            }

            /**
             * 在程序最开始的时候调用
             * 每个并行度调用一次
             * 生命周期开启
             * @param parameters
             * @throws Exception
             */
            @Override
            public void open(Configuration parameters) throws Exception {
                System.out.println("start...");
            }

            @Override
            public String map(String value) throws Exception {
                return value;
            }
        }).setParallelism(2);

        map.print().setParallelism(2);
        env.execute();
    }
}
