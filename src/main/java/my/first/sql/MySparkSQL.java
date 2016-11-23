package my.first.sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Copyright (c) 2012 Conversant Solutions. All rights reserved.
 * <p/>
 * Created on 2016/11/22.
 */
public class MySparkSQL {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder().master("local")//.master("spark://192.168.1.26:7077")
                .appName("Java Spark SQL basic example")
                .config("spark.some.config.option", "some-value")
                .getOrCreate();
        Dataset<Row> df = spark.read().json("src/main/resources/root.log");
        df.printSchema();
    }
}
