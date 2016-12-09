package my.first.sql;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Function1;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Copyright (c) 2012 Conversant Solutions. All rights reserved.
 * <p/>
 * Created on 2016/11/22.
 */
public class MySparkSQL {
    static String log = "2016-11-17 16:44:08,323 [qtp1546335363-4280] INFO [cn.com.conversant.swiftcoder.service.impl.CallbackServiceImpl] - [Start Publish #Step 1] Begin To Callback The API /v1/stream/status/callback [Stream]: 110097_20161117164406";
    public static final Pattern apacheLogRegex =
            Pattern.compile("^([\\d:,\\- ]+) ([\\[\\]\\d\\w\\.\\- ]+ \\- \\[)(\\w+) ([\\w\\# ]+) (\\d)([\\[\\]\\w/: ]+) ([\\d_]+)");
    //2016-11-17 16:44:08,323
    //[qtp1546335363-4280] INFO [cn.com.conversant.swiftcoder.service.impl.CallbackServiceImpl] - [
    //Start
    //Publish #Step
    //1
    //] Begin To Callback The API /v1/stream/status/callback [Stream]:
    //110097_20161117164406

    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder().master("local")//.master("spark://192.168.1.26:7077")
                .appName("Java Spark SQL basic example")
                .config("spark.some.config.option", "some-value")
                .getOrCreate();
        JavaRDD<String> logRdd = spark.read().textFile("src/main/resources/root.log").javaRDD();

        String schemaString = "sn type step date desc";

        List<StructField> fields = new ArrayList<>();
        for(String field : schemaString.split(" ")){
            StructField structField = DataTypes.createStructField(field, DataTypes.StringType, true);
            fields.add(structField);
        }
        StructType schema = DataTypes.createStructType(fields);

        JavaRDD<Row> rowJavaRDD = logRdd.map(new Function<String, Row>() {
            @Override
            public Row call(String s) throws Exception {
                Matcher matcher = apacheLogRegex.matcher(s);
                if(matcher.find()){
                    return RowFactory.create(matcher.group(7), matcher.group(3), matcher.group(5), matcher.group(1), matcher.group(6));
                }
                return null;
            }
        }).filter(new Function<Row, Boolean>() {
            @Override
            public Boolean call(Row row) throws Exception {
                if(row != null){
                    return true;
                }
                return false;
            }
        });

        Dataset<Row> logDataset = spark.createDataFrame(rowJavaRDD, schema);

        logDataset.createOrReplaceTempView("log");

        Dataset<Row> logTables = spark.sql("select * from log");

//        logTables.printSchema();
        logTables.show();

        Dataset<Row> step2 = spark.sql("select count(*) from log where step = 2");
        step2.show();

//        Dataset<Row> dateTime = spark.sql("select * from log where to_char(date, 'yyyyMMdd') = '20161117'");
//        dateTime.show();
    }
}
