package com.atguigu.flinksql_table.functions;

import com.atguigu.day02.source.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

public class TableFunctions {
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
        tableEnv.createTemporarySystemFunction("split", SplitFunction.class);

        //SQL
        tableEnv.executeSql("select id, word from "+table +", lateral table(split(id))").print();
    }
    //自定义UDTF函数将传入的id按照下划线炸裂成两条数据
    //hint暗示，主要作用为类型推断时使用
    @FunctionHint(output = @DataTypeHint("ROW<word STRING>"))
    public static class SplitFunction extends TableFunction<Row> {
        public void eval(String str) {
            for (String s : str.split("_")) {
                collect(Row.of(s));
            }
        }
    }
}
