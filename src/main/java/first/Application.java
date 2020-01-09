package first;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import scala.Tuple3;

import java.util.ArrayList;
import java.util.Arrays;

public class Application {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf()
                .setAppName("Lesson1")
                .setMaster("local[*]");

        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("ERROR");

        //Value initialization
        ArrayList<Integer> myIntegerList = new ArrayList<Integer>();
        myIntegerList.add(2);
        myIntegerList.add(5);
        myIntegerList.add(7);
        myIntegerList.add(12);
        myIntegerList.add(24);

        ArrayList<String> logData = new ArrayList<>();
        logData.add("WARN: Tuesday 4 September 0405");
        logData.add("ERROR: Tuesday 4 September 0408");
        logData.add("FATAL: Wednesday 5 September 1632");
        logData.add("ERROR: Friday 7 September 1854");
        logData.add("WARN: Saturday 8 September 1942");
        logData.add("WARN: Saturday 10 September 1946");
        logData.add("WARN: Saturday 13 September 1949");

        //Input e Map
        partOne(sc, myIntegerList);

        //RDD di oggetti (WRONG)
        partTwo(sc, myIntegerList);

        //RDD di Tuple2 (OK)
        partThree(sc, myIntegerList);

        //PairRDD and reduceByKey (Boring)
        partFour(sc, logData);

        //PairRDD and reduceByKey (FluentAPI)
        partFive(sc, logData);

        //PairRDD and groupByKey (Catastrofic?)
        partSix(sc, logData);

        //FlatMap
        partSeven(sc, logData);

        //Closing SparkContext
        sc.close();

    }

    //Input e Map
    public static void partOne(JavaSparkContext sc, ArrayList<Integer> myIntegerList) {

        JavaRDD<Integer> myRDD = sc.parallelize(myIntegerList);

        System.out.println("Stampo RDD semplice:");
        myRDD.collect().forEach(System.out::println);

        JavaRDD<Integer> doubleRDD = myRDD.map(element -> element * element);

        System.out.println("Stampo RDD al quadrato:");
        doubleRDD.collect().forEach(System.out::println);

    }

    //RDD di oggetti (WRONG)
    public static void partTwo(JavaSparkContext sc, ArrayList<Integer> myIntegerList) {

        JavaRDD<Integer> myRDD = sc.parallelize(myIntegerList);

        System.out.println("\nStampo RDD semplice:");
        myRDD.collect().forEach(System.out::println);

        JavaRDD<ValueWithDouble> valueAndDoubleRDD = myRDD.map(value -> new ValueWithDouble(value));

        valueAndDoubleRDD.collect().forEach(element -> {
            System.out.println("(Object) Value : " + element.getInputValue() + " - Double: " + element.getDoubleValue());
        });

    }

    //RDD di Tuple2 (OK)
    public static void partThree(JavaSparkContext sc, ArrayList<Integer> myIntegerList) {

        JavaRDD<Integer> myRDD = sc.parallelize(myIntegerList);

        System.out.println("\nStampo RDD semplice:");
        myRDD.collect().forEach(System.out::println);

        System.out.println("\nStampo RDD con quadrato:");
        JavaRDD<Tuple2> valueAndDoubleRDD = myRDD.map(value -> new Tuple2(value, value * value));
        valueAndDoubleRDD.collect().forEach(element -> {
            System.out.println("(TUPLE2) Value : " + element._1 + " - Double: " + element._2);
        });

        System.out.println("\nStampo RDD con quadrato e cubo:");
        JavaRDD<Tuple3> valueDoubleAndTripleRDD = myRDD.map(value -> new Tuple3(value, value * value, value * value * value));
        valueDoubleAndTripleRDD.collect().forEach(element -> {
            System.out.println("(TUPLE3) Value : " + element._1() + " - Double: " + element._2() + " - Triple: " + element._3());
        });

    }

    //PairRDD and reduceByKey (Boring)
    public static void partFour(JavaSparkContext sc, ArrayList<String> logData) {

        JavaRDD<String> logMessageRDD = sc.parallelize(logData);

        System.out.println("\nStampo PairRDD:");
        JavaPairRDD<String, String> splittedLogMessageRDD = logMessageRDD.mapToPair(rawValue -> {
            String[] splitted = rawValue.split(":");
            return new Tuple2<>(splitted[0], splitted[1]);
        });
        splittedLogMessageRDD.foreach(element -> {
            System.out.println("LEVEL: " + element._1 + " - MESSAGE: " + element._2);
        });


        System.out.println("\nStampo count del PairRDD per chiave: (Boring)");
        JavaPairRDD<String, Long> splittedLogMessageForCountRDD = logMessageRDD.mapToPair(rawValue -> {
            String[] splitted = rawValue.split(":");
            return new Tuple2<>(splitted[0], 1L);
        });
        splittedLogMessageForCountRDD.reduceByKey((value1, value2) -> value1 + value2)
                .foreach(element -> System.out.println("LEVEL: " + element._1 + " - COUNT: " + element._2));

    }

    //PairRDD and reduceByKey (Fluent)
    public static void partFive(JavaSparkContext sc, ArrayList<String> logData) {

        System.out.println("\nStampo pairRDD con reduceByKey: (Fluent)");
        sc.parallelize(logData)
                .mapToPair(rawValue -> new Tuple2<>(rawValue.split(":")[0], 1L))
                .reduceByKey((value1, value2) -> value1 + value2)
                .foreach(element -> System.out.println("LEVEL: " + element._1 + " - COUNT: " + element._2));
    }

    //PairRDD and groupByKey (Catastrofic?)
    public static void partSix(JavaSparkContext sc, ArrayList<String> logData) {

        System.out.println("\nStampo pairRDD con groupByKey:");
        sc.parallelize(logData)
                .mapToPair(rawValue -> new Tuple2<>(rawValue.split(":")[0], rawValue.split(":")[1]))
                .groupByKey()
                .foreach(element -> System.out.println("LEVEL: " + element._1 + " - MESSAGE: " + element._2));
    }

    //Filter
    public static void partSeven(JavaSparkContext sc, ArrayList<String> logData) {

        System.out.println("\nFilter su RDD:");
        sc.parallelize(logData)
                .mapToPair(rawValue -> new Tuple2<>(rawValue.split(":")[0], rawValue.split(":")[1]))
                .filter(value -> value._1.equals("ERROR"))
                .foreach(element -> System.out.println("LEVEL: " + element._1 + " - MESSAGE: " + element._2));
    }

}
