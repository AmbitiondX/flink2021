package com.atguigu.day02.source;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink03_Source_File_Socket {
    public static void main(String[] args) throws Exception {
        // 获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(2);

        // TODO 从文件中获取数据
//        DataStreamSource<String> textFile = env.readTextFile("input/word.txt").setParallelism(2);
//        textFile.print();

        //TODO 从端口中获取数据
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 7777);

        // 报错：The parallelism of non parallel operator must be 1.
        // DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 7777).setParallelism(2);
        socketTextStream.print();

        env.execute();
    }
}
