package com.atguigu.day09;

import com.atguigu.day02.source.WaterSensor;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;


import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

// todo 官方文档连接： https://nightlies.apache.org/flink/flink-docs-release-1.12/zh/dev/table/functions/udfs.html#%E6%A0%87%E9%87%8F%E5%87%BD%E6%95%B0

public class Flink07_Fun_UDTF {
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
                .joinLateral(call(MyUTDF.class,$("id")))
                .select($("id"),$("word"))
                .execute()
                .print();*/

        // todo 先注册在使用
        // 注册函数
        tableEnv.createTemporarySystemFunction("MyUDTF", MyUDTF.class);
        /*table
                .leftOuterJoinLateral(call("MyUDTF", $("id")))
                .select($("id"),$("word"))
                .execute()
                .print();*/

        // todo 在sql中使用自定义函数
        tableEnv
                // .executeSql("select id,word from " + table + " ,lateral table(MyUDTF(id))" )
                // .executeSql("select id,word from " + table + " left Join lateral table(MyUDTF(id)) on true" )
                // 在sql中重命名函数字段
                .executeSql("select id,neword from " + table + " left join lateral table(MyUDTF(id)) as T(neword) on true")
                .print();
    }

    //自定义一个类实现UDTF(表函数TableFunction)函数 根据id按照空格切分获取到字母以及数字
    @FunctionHint(output = @DataTypeHint("Row<word STRING>"))
    public static class MyUDTF extends TableFunction<Row> {
        public void eval(String str) {
            String[] split = str.split("_");
            for (String s : split) {
                collect(Row.of(s));
            }
        }
    }
}
