package com.atguigu.flinksql_table.functions;

import com.atguigu.day02.source.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

public class AggregateFunctions {
    public static void main(String[] args) {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //2.读取文件得到DataStream
        DataStreamSource<WaterSensor> waterSensorDataStreamSource = env.fromElements(new WaterSensor("sensor_1", 1000L, 10),
                new WaterSensor("sensor_1", 2000L, 20),
                new WaterSensor("sensor_2", 3000L, 30),
                new WaterSensor("sensor_1", 4000L, 40),
                new WaterSensor("sensor_1", 5000L, 50),
                new WaterSensor("sensor_2", 6000L, 60));

        //3.将流转换为动态表
        Table table = tableEnv.fromDataStream(waterSensorDataStreamSource);

        //4先注册再使用
        tableEnv.createTemporarySystemFunction("myavg", MyAvg.class);

        //TableAPI
        table.groupBy($("id"))
                .select($("id"),call("myavg",$("vc")))
                .execute()
                .print();

        //SQL
        tableEnv.executeSql("select id, myavg(vc) from "+table +" group by id").print();
    }

    //定义一个类当做累加器，并声明总数和总个数这两个值
    public static class MyAvgAccumulator{
        public long sum = 0;
        public int count = 0;
    }

    //自定义UDAF函数,求每个WaterSensor中VC的平均值
    public static class MyAvg extends AggregateFunction<Double,MyAvgAccumulator> {

        // 创建一个累加器
        @Override
        public MyAvgAccumulator createAccumulator() {
            return new MyAvgAccumulator();
        }

        //做累加操作
        public void accumulate(MyAvgAccumulator acc, Integer vc) {
            acc.sum += vc;
            acc.count += 1;

        }

        @Override
        public Double getValue(MyAvgAccumulator accumulator) {
            return accumulator.sum*1D / accumulator.count;
        }


    }
}
