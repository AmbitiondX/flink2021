package com.atguigu.flinksql_table.hivecatalog;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

public class ConnectHive {
    public static void main(String[] args) {
        // 设置用户权限
        System.setProperty("HADOOP_USER_NAME", "atguigu");

        // 获取流和表的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        String name            = "myhive";  // Catalog 名字
        String defaultDatabase = "db_hive"; // 默认数据库
        String hiveConfDir     = "c:/conf"; // hive配置文件的目录. 需要把hive-site.xml添加到该目录

        // 1. 创建HiveCatalog
        HiveCatalog hive = new HiveCatalog(name, defaultDatabase, hiveConfDir);
        // 2. 注册HiveCatalog
        tEnv.registerCatalog(name, hive);
        // 3. 把 HiveCatalog: myhive 作为当前session的catalog
        tEnv.useCatalog(name);
        tEnv.useDatabase("db_hive");

        //指定SQL语法为Hive语法
        tEnv.getConfig().setSqlDialect(SqlDialect.HIVE);

        tEnv.sqlQuery("select * from stu").execute().print();
    }
}
