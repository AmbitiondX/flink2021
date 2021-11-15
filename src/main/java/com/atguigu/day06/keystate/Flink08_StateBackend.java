package com.atguigu.day06.keystate;

import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.IOException;

public class Flink08_StateBackend {
    public static void main(String[] args) throws IOException {
        // 获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // TODO 设置状态后端
        //MemoryStateBackend,内存级别的状态后端
        env.setStateBackend(new MemoryStateBackend());
        // FsStateBackend,本地状态在TaskManager内存, Checkpoint存储在文件系统中
        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/flink-ck"));


        /*
            <dependency>
                <groupId>org.apache.flink</groupId>
                <artifactId>flink-statebackend-rocksdb_${scala.binary.version}</artifactId>
                <version>${flink.version}</version>
                <scope>provided</scope>
            </dependency>
         */
        // 将所有的状态序列化之后, 存入本地的RocksDB数据库中.(一种NoSql数据库, KV形式存储)
        // 第二个参数为true时，在写入状态时是否为增量写
        env.setStateBackend(new RocksDBStateBackend("hdfs://hadoop102:8020/flink-ck-RD"));
    }
}
