package my.first;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import org.codehaus.janino.Java;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Copyright (c) 2012 Conversant Solutions. All rights reserved.
 * <p/>
 * Created on 2016/10/27.
 */
public class JavaWordCount {

    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) throws Exception {

//        if (args.length < 1) {
//            System.err.println("Usage: JavaWordCount <file>");
//            System.exit(1);
//        }

        SparkSession spark = SparkSession
                .builder().master("spark://192.168.1.26:7077")
                .appName("JavaWordCount")
                .getOrCreate();
        spark.sparkContext().addJar("E:\\work\\workspace\\spark-study\\target\\spark-study-1.0-SNAPSHOT.jar");
        JavaRDD<String> lines = spark.read().textFile("hdfs://192.168.1.26:9002/logback/2016-11-17/17/dvr-.*").javaRDD();

//        SparkSession spark = SparkSession
//                .builder().master("local")
//                .appName("JavaWordCount")
//                .getOrCreate();
//        JavaRDD<String> lines = spark.read().textFile(args[0]).javaRDD();
//
        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) {
                return Arrays.asList(SPACE.split(s)).iterator();
            }
        });

//        words.persist(StorageLevel.DISK_ONLY());

//        words = words.filter(new Function<String, Boolean>() {
//            @Override
//            public Boolean call(String s) throws Exception {
//                if(s.equals("hadoop") || s.equals("huo")){
//                    return false;
//                }
//                return true;
//            }
//        });

        JavaPairRDD<String, Integer> ones = words.mapToPair(
                new PairFunction<String, String, Integer>() {
                    @Override
                    public Tuple2<String, Integer> call(String s) {
                        return new Tuple2<>(s, 1);
                    }
                });

        JavaPairRDD<String, Integer> counts = ones.reduceByKey(
                new Function2<Integer, Integer, Integer>() {
                    @Override
                    public Integer call(Integer i1, Integer i2) {
                        return i1 + i2;
                    }
                });

//        counts.saveAsTextFile("e:/result.txt");

        List<Tuple2<String, Integer>> output = counts.collect();
        for (Tuple2<?,?> tuple : output) {
            System.out.println(tuple._1() + ": " + tuple._2());
        }
        spark.stop();
    }
}
