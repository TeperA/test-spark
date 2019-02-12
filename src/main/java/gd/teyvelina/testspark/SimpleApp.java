package gd.teyvelina.testspark;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;

public class SimpleApp {
    public static void main(String[] args) {
        String logFile = "/usr/local/spark/README.md"; // Should be some file on your system
        SparkSession spark = SparkSession.builder().appName("Simple Application").getOrCreate();
        Dataset<String> logData = spark.read().textFile(logFile).cache();

        long numAs = logData.filter(s -> s.contains("a")).count();
        long numBs = logData.filter(s -> s.contains("b")).count();

        System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);
        spark.stop();
    }
}

//    private static List<String> call(String line) {
//        return Arrays.asList(line.split(" "));
//    }

/*/usr/local/spark/bin/spark-submit --class "gd.teyvelina.testspark.SimpleApp" --master local[4]*/