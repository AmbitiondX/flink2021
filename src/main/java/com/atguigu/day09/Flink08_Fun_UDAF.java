package com.atguigu.day09;

import com.atguigu.day02.source.WaterSensor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

// todo 官方文档连接： https://nightlies.apache.org/flink/flink-docs-release-1.12/zh/dev/table/functions/udfs.html#%E6%A0%87%E9%87%8F%E5%87%BD%E6%95%B0

public class Flink08_Fun_UDAF {
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
                .select($("id"),call(MyUDAF.class, $("vc")))
                .execute()
                .print();*/

        // todo 先注册在使用
        // 注册函数
        tableEnv.createTemporarySystemFunction("MyUDAF", MyUDAF.class);
        /*table
                .groupBy($("id"))
                .select($("id"),call("MyUDAF", $("vc")))
                .execute()
                .print();*/

        // todo 在sql中使用自定义函数
        tableEnv
                .executeSql("select id,MyUDAF(vc) from " + table + " group by id")
                .print();
    }

    // todo 自定义一个类实现UDAF(表函数AggFunction)函数 根据vc求出平均值

    // 定义一个累加器的类
    public static class AvgAcc{
        public long sum = 0;
        public int count = 0;
    }

    public static class MyUDAF extends AggregateFunction<Double,AvgAcc>{

        // 计算最后输出的值
        @Override
        public Double getValue(AvgAcc accumulator) {
            return accumulator.sum * 1D / accumulator.count;
        }

        // 创建一个累加器
        @Override
        public AvgAcc createAccumulator() {
            return new AvgAcc();
        }

        // 累加过程
        public void accumulate(AvgAcc acc, Integer value){
            acc.sum += value;
            acc.count++;
        }

    }
}
