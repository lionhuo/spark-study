package my.first;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
import scala.Tuple3;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Copyright (c) 2012 Conversant Solutions. All rights reserved.
 * <p/>
 * Created on 2016/11/18.
 */
public class MyLogQuery {
    //2016-11-17 16:44:08,323 [qtp1546335363-4280] INFO  [cn.com.conversant.swiftcoder.service.impl.CallbackServiceImpl] - [Start Publish #Step 1] Begin To Callback The API /v1/stream/status/callback [Stream]: 110097_20161117164406
    static String log = "2016-11-17 16:44:08,323 [qtp1546335363-4280] INFO [cn.com.conversant.swiftcoder.service.impl.CallbackServiceImpl] - [Start Publish #Step 1] Begin To Callback The API /v1/stream/status/callback [Stream]: 110097_20161117164406";
    public static final Pattern apacheLogRegex = Pattern.compile("^.*110149_20161118141405.*");    // (\S+) (\S+) \[([\w.]+)\]

    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder().master("local").appName("myLogQuery").getOrCreate();
        JavaRDD<String> lines = sparkSession.read().textFile("hdfs://192.168.1.26:9002/logback/2016-11-18/sc-*").javaRDD();

        JavaPairRDD<String,String> counts = lines.mapToPair(new PairFunction<String, String, String>(){
            @Override
            public Tuple2<String, String> call(String s) throws Exception {
                Matcher matcher = apacheLogRegex.matcher(s);
                if(matcher.find()){
                    return new Tuple2<String, String>(s, "");
                }
                return new Tuple2<String, String>(null, "");
            }
        });

//        JavaPairRDD<String, String> results = counts.reduceByKey(new Function2<String, String, String>() {
//            @Override
//            public String call(String s, String s2) throws Exception {
//                return null;
//            }
//        });

        List<Tuple2<String, String>> output = counts.collect();
        for (Tuple2<?,?> t : output) {
            if(t._1() != null){
                System.out.println(t._1());
            }
        }
        sparkSession.stop();
    }
}
