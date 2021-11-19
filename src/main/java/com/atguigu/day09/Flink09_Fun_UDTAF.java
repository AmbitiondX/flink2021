package com.atguigu.day09;

import com.atguigu.day02.source.WaterSensor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.util.Collector;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

// todo 官方文档连接： https://nightlies.apache.org/flink/flink-docs-release-1.12/zh/dev/table/functions/udfs.html#%E6%A0%87%E9%87%8F%E5%87%BD%E6%95%B0

public class Flink09_Fun_UDTAF {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        // 从端口获取数据
        SingleOutputStreamOperator<WaterSensor> waterSensorDStream = env
                .socketTextStream("localhost", 7777)
                .map(line -> {
                    String[] split = line.split(" ");
                    return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
                });

        // 创建表的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 将流转化为表
        Table table = tableEnv.fromDataStream(waterSensorDStream);

        // todo 不注册直接使用函数
        /*table
                .groupBy($("id"))
                .flatAggregate(call(MyUDTAF.class, $("vc")).as("value", "rank"))
                .select($("id"),$("value"),$("rank"))
                .execute()
                .print();*/

        // todo 先注册在使用
        // 注册函数
        tableEnv.createTemporarySystemFunction("MyUDTAF", MyUDTAF.class);
        table
                .groupBy($("id"))
                .flatAggregate("MyUDTAF(vc) as (value,rank)")
                .select("id,value,rank")
                .execute()
                .print();

        // todo 不能在sql中使用自定义UDTAF函数
    }

    // todo 自定义一个类实现UDAF(表函数AggFunction)函数 根据vc求出平均值
    // 定义一个累加器的类
    public static class Top2Accum{
        public Integer first = Integer.MIN_VALUE;
        public Integer second = Integer.MIN_VALUE;
    }

    public static class MyUDTAF extends TableAggregateFunction<Tuple2<Integer,Integer>,Top2Accum>{

        @Override
        public Top2Accum createAccumulator() {
            return new Top2Accum();
        }

        public void accumulate(Top2Accum acc, Integer value){
            if (value > acc.first) {
                acc.second = acc.first;
                acc.first = value;
            } else if (value > acc.second) {
                acc.second = value;
            }
        }

        public void emitValue(Top2Accum acc, Collector<Tuple2<Integer,Integer>> out){
            if (acc.first != Integer.MIN_VALUE) {
                out.collect(Tuple2.of(acc.first, 1));
            }
            if (acc.second != Integer.MIN_VALUE) {
                if (acc.first == acc.second){
                    out.collect(Tuple2.of(acc.second, 1));
                } else {
                    out.collect(Tuple2.of(acc.second, 2));
                }

            }
        }
    }
}
