package coLocMining;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class CountEvents {

    private static void wordCount(String fileName) {

//        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("JD Word Counter");
//
//        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
//
//        JavaRDD<String> inputFile = sparkContext.textFile(fileName);
//
////        JavaRDD<String> EventsFromFile = inputFile.flatMap(content -> Arrays.asList(content.split(" ")));
//
//        JavaPairRDD countData = EventsFromFile.mapToPair(t -> new Tuple2(t, 1)).reduceByKey((x, y) -> (int) x + (int) y);
//
//        countData.saveAsTextFile("CountData");
    }


}
