package com.atguigu.day09;

import com.atguigu.day02.source.WaterSensor;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;

// todo 官方文档连接： https://nightlies.apache.org/flink/flink-docs-release-1.12/zh/dev/table/functions/udfs.html#%E6%A0%87%E9%87%8F%E5%87%BD%E6%95%B0

public class Flink06_Fun_UDF {
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
                .select($("id"),call(MyUDF.class,$("id")))
                .execute()
                .print();*/

        // todo 先注册在使用
        // 注册函数
        tableEnv.createTemporarySystemFunction("strLen", MyUDF.class);
        /*table
                .select($("id"),call("strLen", $("id")))
                .execute()
                .print();*/

        // todo 在sql中使用自定义函数
        tableEnv
                .executeSql("select id, strLen(id) from " + table )
                .print();
    }

    //自定义一个类实现UDF(标量函数ScalarFunction)函数 根据id获取id的字符串长度
    public static class MyUDF extends ScalarFunction{
        public int eval (String str) {
            return str.length();
        }
    }
}
