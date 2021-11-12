package com.atguigu.day03.sink_test;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.day02.source.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
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
        DataStreamSource<String> source = env.socketTextStream("localhost", 7777);
        SingleOutputStreamOperator<String> jsonStringStream = source.map(new MapFunction<String, String>() {
            @Override
            public String map(String s) throws Exception {
                String[] split = s.split(" ");
                String jsonString = JSONObject.toJSONString(new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2])));
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

       // todo 将数据写入es
        ArrayList<HttpHost> httpHosts = new ArrayList<HttpHost>();
        httpHosts.add(new HttpHost("hadoop102", 9200));
        ElasticsearchSink.Builder<String> stringBuilder = new ElasticsearchSink.Builder<>(httpHosts, new ElasticsearchSinkFunction<String>() {
            @Override
            public void process(String s, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {
                String id = JSONObject.parseObject(s).getString("id");
                IndexRequest indexRequest = new IndexRequest("0625", "_doc", id).source(s, XContentType.JSON);
                requestIndexer.add(indexRequest);
            }
        });
        stringBuilder.setBulkFlushMaxActions(1);
        jsonStringStream.addSink(stringBuilder.build());
        env.execute();

    }
}
