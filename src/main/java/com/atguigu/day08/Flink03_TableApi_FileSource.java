package com.atguigu.day08;

import com.atguigu.day02.source.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

public class Flink03_TableApi_FileSource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        // 获取表的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        Schema schema = new Schema()
                .field("id", DataTypes.STRING())
                .field("ts", DataTypes.BIGINT())
                .field("vc", DataTypes.INT());

        // 连接文件 创建一张临时表(动态表)
        tableEnv.connect(new FileSystem().path("input/sensor-sql.txt"))
                .withFormat(new Csv().fieldDelimiter(',').lineDelimiter("\n"))
                .withSchema(schema)
                .createTemporaryTable("sensor");

        // 将临时表做成表对象，然后对动态表进行查询
        /*Table sensorTable = tableEnv.from("sensor");
        Table resultTable = sensorTable
                .where($("vc").isGreaterOrEqual(20))
                .groupBy($("id"))
                .select($("id"), $("vc").sum());*/

        // 使用sql的方式查询
        Table resultTable = tableEnv.sqlQuery("select * from sensor");



        // 将表追加到流当中，打印出来
        tableEnv.toRetractStream(resultTable, Row.class).print();

        env.execute();
    }
}
