package ru.mephi.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import ru.mephi.spark.data.Util;
import scala.Tuple2;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static ru.mephi.spark.data.Util.parseLog;


public class AirportComputeTest {

    private JavaSparkContext sparkContext;

    JavaPairRDD<String, String> files;
    JavaRDD<List<String>> logCountsJavaRDD;
    private List<String> expectedOutput = new ArrayList<>();

    @Test
    public void testLogAggregation() {
        SparkConf conf = new SparkConf();
        conf.setMaster("local[2]");
        conf.setAppName("junit");
        sparkContext = new JavaSparkContext(conf);
        expectedOutput.add("19:00:00, Ukraine, Colombia,1");
        expectedOutput.add("3:00:00, Ukraine, Belize,1");
        expectedOutput.add("19:00:00, Ukraine, Grenada,2");
        expectedOutput.add("1:00:00, Ukraine, Lebanon,1");
        expectedOutput.add("9:00:00, Ukraine, Romania,1");
        expectedOutput.add("15:00:00, Ukraine, Burkina Faso,1");
        expectedOutput.add("0:00:00, Ukraine, Malawi,1");
        expectedOutput.add("20:00:00, Ukraine, Yemen,1");
        expectedOutput.add("14:00:00, Ukraine, Aruba,1");
        JavaPairRDD<String, String> files = sparkContext.wholeTextFiles("src/test/resources/DMD-Ukraine");
        JavaRDD<List<String>> logCountsJavaRDD = files.map(Util::parseLog);
        List<List<String>> resultList = logCountsJavaRDD.collect();
        assertEquals(expectedOutput.size(), resultList.get(0).size());
        assertTrue(resultList.contains(expectedOutput));
    }
}