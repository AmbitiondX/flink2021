package com.atguigu.day04.project;

import com.atguigu.day04.project.bean.UserBehavior;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashSet;

public class Flink02_Project_UV {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> source = env.readTextFile("input/UserBehavior.csv");
        SingleOutputStreamOperator<UserBehavior> userBehaviorStream = source.map(new MapFunction<String, UserBehavior>() {
            @Override
            public UserBehavior map(String value) throws Exception {
                String[] split = value.split(",");
                return new UserBehavior(Long.parseLong(split[0]), Long.parseLong(split[1]), Integer.parseInt(split[2]), split[3], Long.parseLong(split[4]));
            }
        });


        HashSet<Long> set = new HashSet<>();
        SingleOutputStreamOperator<UserBehavior> filter = userBehaviorStream.filter(new FilterFunction<UserBehavior>() {
            @Override
            public boolean filter(UserBehavior value) throws Exception {
                if ("pv".equals(value.getBehavior())) {
                    boolean bool = set.contains(value.getUserId());
                    if (bool) {
                        return false;
                    } else {
                        set.add(value.getUserId());
                        return true;
                    }
                }
                return false;
            }
        });


        filter.process(new ProcessFunction<UserBehavior, Integer>() {
            private Integer count = 0;
            @Override
            public void processElement(UserBehavior value, ProcessFunction<UserBehavior, Integer>.Context ctx, Collector<Integer> out) throws Exception {
                count ++;
                out.collect(count);
            }
        }).print();

        env.execute();
    }
}
