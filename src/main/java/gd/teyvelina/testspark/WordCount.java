package gd.teyvelina.testspark;


import gd.teyvelina.RandomString;
import org.apache.hadoop.mapred.FileAlreadyExistsException;
//import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;


//import java.nio.file.FileAlreadyExistsException;
import java.util.*;


public class WordCount {
    public static void main(String[] args) {

        // create Spark context with Spark configuration
        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("Word Count").setMaster("local"));

        //https://blog.cloudera.com/blog/2014/04/making-apache-spark-easier-to-use-in-java-with-java-8/
        // count words
        JavaRDD<String> lines = sc.textFile("data.txt");
        JavaRDD<String> words = lines.flatMap(x -> Arrays.asList(x.split(",")).iterator());
        JavaPairRDD<String, Integer> counts = words.mapToPair(w -> new Tuple2<String, Integer>(w, 1))
                .reduceByKey((x, y) -> x + y);
        RandomString randomString = new RandomString(8);
        //TODO: timestamp
        try {
            counts.saveAsTextFile("output");
        } catch (Exception e) {
            if (e.getClass().getName().equals("org.apache.hadoop.mapred.FileAlreadyExistsException"))
                counts.saveAsTextFile("output" + randomString.toString());
        }

        System.out.println(counts.count());

        sc.stop();

        // count characters
        /*JavaPairRDD<Character, Integer> charCounts = counts.flatMap(
                (FlatMapFunction<Tuple2<String, Integer>, Character>) s -> {
                    Collection<Character> chars = new ArrayList<Character>(s._1().length());
                    for (char c : s._1().toCharArray()) {
                        chars.add(c);
                    }
                    return (Iterator<Character>) chars;
                }
        ).mapToPair((PairFunction<Character, Character, Integer>) c -> new Tuple2<Character, Integer>(c, 1)
        ).reduceByKey((Function2<Integer, Integer, Integer>) (i1, i2) -> i1 + i2);

        System.out.println(charCounts.collect());*/
    }
}
