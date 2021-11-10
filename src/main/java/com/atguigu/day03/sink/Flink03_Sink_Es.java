package com.atguigu.day03.sink;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.day02.source.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.xcontent.XContentType;

import java.util.ArrayList;

public class Flink03_Sink_Es {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.从端口读取数据
        SingleOutputStreamOperator<WaterSensor> waterSensorDStream = env.socketTextStream("hadoop102", 7777)
                .map(new MapFunction<String, WaterSensor>() {
                    @Override
                    public WaterSensor map(String value) throws Exception {
                        String[] split = value.split(" ");
                        return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
                    }
                });
        SingleOutputStreamOperator<String> map = waterSensorDStream.map(new MapFunction<WaterSensor, String>() {
            @Override
            public String map(WaterSensor value) throws Exception {
                String jsonString = JSONObject.toJSONString(value);
                return jsonString;
            }
        });

        /*
            导入依赖
            <dependency>
                <groupId>org.apache.flink</groupId>
                <artifactId>flink-connector-elasticsearch6_2.11</artifactId>
                <version>1.12.0</version>
            </dependency>
         */

        ArrayList<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("hadoop102",9200));
        ElasticsearchSink.Builder<String> builder = new ElasticsearchSink.Builder<>(httpHosts, new ElasticsearchSinkFunction<String>() {
            @Override
            public void process(String element, RuntimeContext ctx, RequestIndexer indexer) {
                IndexRequest indexRequest = new IndexRequest().index("0625").type("_doc");
                // 获取字符串中的id，并设置为es中的id
                String id = JSONObject.parseObject(element).getString("id");
                indexRequest.id(id);
                // XContentType：设置内容的类型，才能真确识别
                indexRequest.source(element, XContentType.JSON);
                indexer.add(indexRequest);
            }
        });
        builder.setBulkFlushMaxActions(1);
        map.addSink(builder.build());

        env.execute();

    }
}
