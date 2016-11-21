/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package my.first.sql.hive;

// $example on:spark_hive$

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
// $example off:spark_hive$

public class JavaSparkHiveExample {

  // $example on:spark_hive$
  public static class Record implements Serializable {
    private int key;
    private String value;

    public int getKey() {
      return key;
    }

    public void setKey(int key) {
      this.key = key;
    }

    public String getValue() {
      return value;
    }

    public void setValue(String value) {
      this.value = value;
    }
  }
  // $example off:spark_hive$

  public static void main(String[] args) {
    // $example on:spark_hive$
    // warehouseLocation points to the default location for managed databases and tables
    String warehouseLocation = "hdfs://192.168.1.26:9002/user/hive/warehouse";//"spark-warehouse";
    SparkSession spark = SparkSession
      .builder().master("local")//.master("spark://192.168.1.26:7077")//
      .appName("Java Spark Hive Example")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport()
      .getOrCreate();
//    spark.sparkContext().addJar("E:\\work\\workspace\\spark-study\\target\\spark-study-1.0-SNAPSHOT.jar");//,E:\\work\\soft\\apache-maven-3.3.9\\resp\\org\\apache\\spark\\spark-hive_2.11\\2.0.1\\spark-hive_2.11-2.0.1.jar");

    spark.sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING)");
    spark.sql("LOAD DATA LOCAL INPATH 'src/main/resources/kv1.txt' INTO TABLE src");

    // Queries are expressed in HiveQL
    spark.sql("SELECT * FROM src").show();
    // +---+-------+
    // |key|  value|
    // +---+-------+
    // |238|val_238|
    // | 86| val_86|
    // |311|val_311|
    // ...

    // Aggregation queries are also supported.
    spark.sql("SELECT COUNT(*) FROM src").show();
    // +--------+
    // |count(1)|
    // +--------+
    // |    500 |
    // +--------+

    // The results of SQL queries are themselves DataFrames and support all normal functions.
    Dataset<Row> sqlDF = spark.sql("SELECT key, value FROM src WHERE key < 10 ORDER BY key");
    sqlDF.show();

    // The items in DaraFrames are of type Row, which lets you to access each column by ordinal.
    Dataset<String> stringsDS = sqlDF.map(new MapFunction<Row, String>() {
      @Override
      public String call(Row row) throws Exception {
        return "Key: " + row.get(0) + ", Value: " + row.get(1);
      }
    }, Encoders.STRING());
    stringsDS.show();
    // +--------------------+
    // |               value|
    // +--------------------+
    // |Key: 0, Value: val_0|
    // |Key: 0, Value: val_0|
    // |Key: 0, Value: val_0|
    // ...

    // You can also use DataFrames to create temporary views within a SparkSession.
    List<Record> records = new ArrayList<>();
    for (int key = 1; key < 100; key++) {
      Record record = new Record();
      record.setKey(key);
      record.setValue("val_" + key);
      records.add(record);
    }
    Dataset<Row> recordsDF = spark.createDataFrame(records, Record.class);
    recordsDF.createOrReplaceTempView("records");

    // Queries can then join DataFrames data with data stored in Hive.
    spark.sql("SELECT * FROM records r JOIN src s ON r.key = s.key").show();
    // +---+------+---+------+
    // |key| value|key| value|
    // +---+------+---+------+
    // |  2| val_2|  2| val_2|
    // |  2| val_2|  2| val_2|
    // |  4| val_4|  4| val_4|
    // ...
    // $example off:spark_hive$

    spark.stop();
  }
}