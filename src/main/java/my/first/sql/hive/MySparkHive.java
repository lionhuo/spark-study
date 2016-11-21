package my.first.sql.hive;

import org.apache.spark.sql.SparkSession;

/**
 * Copyright (c) 2012 Conversant Solutions. All rights reserved.
 * <p/>
 * Created on 2016/11/21.
 */
public class MySparkHive {

    public static void main(String[] args) {
        String warehouseLocation = "hdfs://192.168.1.26:9002/user/hive/warehouse";//"spark-warehouse";
        SparkSession spark = SparkSession
                .builder().master("local")//.master("spark://192.168.1.26:7077")//
                .appName("Java Spark Hive Example")
                .config("spark.sql.warehouse.dir", warehouseLocation)
                .enableHiveSupport()
                .getOrCreate();
//    spark.sparkContext().addJar("E:\\work\\workspace\\spark-study\\target\\spark-study-1.0-SNAPSHOT.jar");//,E:\\work\\soft\\apache-maven-3.3.9\\resp\\org\\apache\\spark\\spark-hive_2.11\\2.0.1\\spark-hive_2.11-2.0.1.jar");

        spark.sql("CREATE TABLE IF NOT EXISTS log (key STRING, value STRING)");
        spark.sql("LOAD DATA LOCAL INPATH 'src/main/resources/root.log' INTO TABLE log");

        // Queries are expressed in HiveQL
        spark.sql("SELECT * FROM log").show();
    }
}
