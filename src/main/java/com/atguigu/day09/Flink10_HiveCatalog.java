package com.atguigu.day09;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

public class Flink10_HiveCatalog {
    public static void main(String[] args) {
        // todo 注意pom依赖的问题   Hive Dependency的依赖放在最后面
        // 设置访问的用户
        // 设置用户权限
        System.setProperty("HADOOP_USER_NAME","atguigu");

        // 1.获取流和表的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        String name            = "myhive";  // Catalog 名字
        String defaultDatabase = "db_hive"; // 默认数据库
        String hiveConfDir     = "c:/conf"; // hive配置文件的目录. 需要把hive-site.xml添加到该目录

        // 3.创建HiveCatalog
        HiveCatalog hiveCatalog = new HiveCatalog(name, defaultDatabase, hiveConfDir);

        // 4.注册HiveCataLog
        tableEnv.registerCatalog(name, hiveCatalog);

        // 设置相关参数
        tableEnv.useCatalog(name);
        tableEnv.useDatabase(defaultDatabase);

        // 指定sql语法为hive
        // Note: We might never support all of the Hive grammar. See the documentation for supported features.
        tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);

        tableEnv
                .executeSql("show tables")
                .print();
    }
}
