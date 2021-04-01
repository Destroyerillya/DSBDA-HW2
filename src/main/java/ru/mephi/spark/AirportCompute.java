package ru.mephi.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import ru.mephi.spark.data.Util;
import scala.Tuple2;
import java.util.*;


public class AirportCompute {

    /**
     * Aggregate logs by hours
     * @param logInfoRDD Source logs
     * @return Aggregated logs

    static JavaRDD<LogCounts> countLogs(JavaRDD<List<LinuxLog>> logInfoRDD)
    {
        return logInfoRDD
                //Map log to key-value pairs, using log as key, and integer 1 (count) as value
                .mapToPair((info) -> new Tuple2<>(new LinuxLog(info.getLogtime(), info.getSrccountry(), info.getDstcountry()), 1))
                //Reduce key-value pairs to sum all log counts' per key
                .reduceByKey(Integer::sum)
                //Map key-value pairs to output class objects
                .map((tuple) -> new LogCounts(tuple._1.getLogtime(), tuple._1.getSrccountry(), tuple._1.getDstcountry(), tuple._2));
    }*/

    public static void main(String[] args)
    {
        SparkConf sparkConf = new SparkConf()
                .setAppName("linux-logs");

        //Create a spark context based on created configuration
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        //JavaRDD<String> file = sparkContext.textFile("hdfs://localhost:9000/user/root/input");

        JavaPairRDD<String, String> file = sparkContext.wholeTextFiles("hdfs://localhost:9000/user/root/input");

        //JavaRDD<LogCounts> logCountsJavaRDD = countLogs(file.map(Util::parseLog), file.name());

        JavaRDD<List<String>> logCountsJavaRDD = file.map(Util::parseLog);

        logCountsJavaRDD.saveAsTextFile("hdfs://localhost:9000/user/root/output");
    }
}
