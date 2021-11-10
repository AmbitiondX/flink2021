package com.atguigu.day03.transform;

import com.atguigu.day02.source.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink04_TransForm_MaxBy {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        // 从端口读取数据，并转为JavaBean
        KeyedStream<WaterSensor, Tuple> keyBy = env.socketTextStream("hadoop102", 7777)
                .map(new MapFunction<String, WaterSensor>() {
                    @Override
                    public WaterSensor map(String value) throws Exception {
                        String[] split = value.split(" ");
                        return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
                    }
                })
                .keyBy("id");
        // .max 分组内，取聚合值的结果，其他保持不变
        // SingleOutputStreamOperator<WaterSensor> result = keyBy.max("vc");

        // .maxby(,true) 当第二个参数为true时(默认值)，分组内，取最新的聚合结果，如果聚合值一样，则取'最老'的结果
        // SingleOutputStreamOperator<WaterSensor> result = keyBy.maxBy("vc", true);

        // .maxby(,false) 当第二个参数为false时，分组内，取最新的聚合结果，如果聚合值一样，则取'最新'的结果
        SingleOutputStreamOperator<WaterSensor> result = keyBy.maxBy("vc", false);

        result.print();

        env.execute();
    }
}
