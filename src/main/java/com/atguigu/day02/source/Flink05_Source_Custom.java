package com.atguigu.day02.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

public class Flink05_Source_Custom {
    private static Boolean bool = true;
    private static Random random = new Random();
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<WaterSensor> source = env.addSource(new Mysource());

        source.print();
        env.execute();
    }

    // 如果想要设置多并行度，则需要实现ParallelSourceFunction
    // public static class MySource implements ParallelSourceFunction<WaterSensor> {
    private static class Mysource implements SourceFunction<WaterSensor> {
        @Override
        public void run(SourceContext ctx) throws Exception {
            while (bool){
                Thread.sleep(200);
                ctx.collect(new WaterSensor(String.valueOf(random.nextInt(100)),random.nextLong(),random.nextInt()));
            }
        }

        /**
         * 取消数据生成和发送
         * cancel()方法是由系统内部自己调的
         */
        @Override
        public void cancel() {
            bool = false;
        }
    }
}
