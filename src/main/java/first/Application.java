package first;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import scala.Tuple2;
import scala.Tuple3;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Application {

    public static void main(String[] args) {

        // Se utilizzate Windows aggiungete il file wintools.exe
        // presente nella cartella winutils-extra nella cartella c:/hadoop
        // (O dove preferite, coerenti con la variabile d'ambiente)

        // Commentare se settate le variabili d'ambiente di hadoop
        System.setProperty("hadoop.home.dir", "c:/hadoop");

        // Definisco la configurazione di Spark per farla lavorare in locale
        SparkConf conf = new SparkConf()
                .setAppName("Lesson1")
                .setMaster("local[*]");

        // Definisco lo SparkContext
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Setto il logger a livello di errore, così da eliminare le stampe "inutili"
        // Se non dovesse bastare uso il file di configurazione di log4j
        sc.setLogLevel("ERROR");

        // Inizializzo i valori per l'array di interi
        ArrayList<Integer> myIntegerList = new ArrayList<Integer>();
        myIntegerList.add(2);
        myIntegerList.add(5);
        myIntegerList.add(7);
        myIntegerList.add(12);
        myIntegerList.add(24);

        // Inizializzo i valori per l'array di string
        ArrayList<String> logData = new ArrayList<>();
        logData.add("WARN: Tuesday 4 September 0405");
        logData.add("ERROR: Tuesday 4 September 0408");
        logData.add("FATAL: Wednesday 5 September 1632");
        logData.add("ERROR: Friday 7 September 1854");
        logData.add("WARN: Saturday 8 September 1942");
        logData.add("WARN: Saturday 10 September 1946");
        logData.add("WARN: Saturday 13 September 1949");

        //Input e Map
        /*partOne(sc, myIntegerList);

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

        //Loading from disk
        partEight(sc);*/

        //Joins
        partNine(sc);

        //Closing SparkContext
        sc.close();

    }

    //Input e Map
    public static void partOne(JavaSparkContext sc, ArrayList<Integer> myIntegerList) {

        // Inizializzo il mio RDD partendo da un array
        JavaRDD<Integer> myRDD = sc.parallelize(myIntegerList);

        System.out.println("Stampo RDD semplice:");
        // Collect per raccogliere tutto il flusso sullo sparkcontext
        // Collect da utilizzare solo quando necessario
        // Lambda per stampare ogni valore
        myRDD.collect().forEach(System.out::println);

        // Definisco un nuovo RDD partendo dal precedente sostituendo il valore con il suo quadrato
        JavaRDD<Integer> doubleRDD = myRDD.map(element -> element * element);

        System.out.println("Stampo RDD al quadrato:");
        doubleRDD.collect().forEach(System.out::println);

    }

    //RDD di oggetti (WRONG)
    public static void partTwo(JavaSparkContext sc, ArrayList<Integer> myIntegerList) {

        JavaRDD<Integer> myRDD = sc.parallelize(myIntegerList);

        System.out.println("\nStampo RDD semplice:");
        myRDD.collect().forEach(System.out::println);

        // Definisco un nuovo RDD mappando il valore in un oggetto Java che DEVE essere serializzabile.
        // Provate a levare implements Serializable e vedrete l'errore
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
        // Definisco un nuovo RDD mappando il valore in un oggetto Tuple2 che è standard per Spark.
        // L'oggetto gode di tante proprietà ed è ottimizzato per Spark. Per accedere ai valori
        // nome_dell_oggetto._x() dove x è il numero dell'intero da 1 a 22.
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
        // Definisco un nuovo RDD che sarà sempre un Tuple2 poiché utilizza una mapToPair
        // che ha come scopo quello di definire un Tuple2<K,V> con K chiave e V valore.
        JavaPairRDD<String, String> splittedLogMessageRDD = logMessageRDD.mapToPair(rawValue -> {
            String[] splitted = rawValue.split(":");
            return new Tuple2<>(splitted[0], splitted[1]);
        });
        // Accedo al RDD come fosse un tuple2 qualsiasi
        splittedLogMessageRDD.foreach(element -> {
            System.out.println("LEVEL: " + element._1 + " - MESSAGE: " + element._2);
        });

        System.out.println("\nStampo count del PairRDD per chiave: (Boring)");
        // Prendiamo confidenza con gli RDD provando a contare il numero degli elementi
        // per livello di errore inserendo 1 come valore all'interno del campo V
        JavaPairRDD<String, Long> splittedLogMessageForCountRDD = logMessageRDD.mapToPair(rawValue -> {
            String[] splitted = rawValue.split(":");
            return new Tuple2<>(splitted[0], 1L);
        });
        // Applico la reduceByKey per contare gli elementi per ogni chiave K
        splittedLogMessageForCountRDD.reduceByKey((value1, value2) -> value1 + value2)
                .foreach(element -> System.out.println("LEVEL: " + element._1 + " - COUNT: " + element._2));

    }

    //PairRDD and reduceByKey (Fluent)
    public static void partFive(JavaSparkContext sc, ArrayList<String> logData) {

        System.out.println("\nStampo pairRDD con reduceByKey: (Fluent)");
        // Sfruttiamo il concetto del FluentAPI contraendo il flusso di operazioni
        // in un unico blocco e rendendo più snello il codice
        sc.parallelize(logData)
                .mapToPair(rawValue -> new Tuple2<>(rawValue.split(":")[0], 1L))
                .reduceByKey((value1, value2) -> value1 + value2)
                .foreach(element -> System.out.println("LEVEL: " + element._1 + " - COUNT: " + element._2));
    }

    //PairRDD and groupByKey (Catastrofic?)
    public static void partSix(JavaSparkContext sc, ArrayList<String> logData) {

        System.out.println("\nStampo pairRDD con groupByKey:");
        // In questo caso la groupByKey è poco pericolosa, in quanto il flusso di dati è irrisorio
        // Va comunque tenuto conto del codice in termini di sviluppo su grandi moli di dati in ambienti distribuiti
        // La groupByKey ha bisogno di raccogliere tutti i dati, passare per un'operazione di Shuffling e ridistribuire
        sc.parallelize(logData)
                .mapToPair(rawValue -> new Tuple2<>(rawValue.split(":")[0], rawValue.split(":")[1]))
                .groupByKey()
                .foreach(element -> System.out.println("LEVEL: " + element._1 + " - MESSAGE: " + element._2));
    }

    //Filter RDD
    public static void partSeven(JavaSparkContext sc, ArrayList<String> logData) {

        System.out.println("\nFilter su RDD:");
        // Vediamo la funzione di filtraggio che si aspetta una risposta booleana in output.
        // Se negativa il dato viene eliminato, se positiva tenuto
        sc.parallelize(logData)
                .mapToPair(rawValue -> new Tuple2<>(rawValue.split(":")[0], rawValue.split(":")[1]))
                .filter(value -> value._1.equals("ERROR"))
                .foreach(element -> System.out.println("LEVEL: " + element._1 + " - MESSAGE: " + element._2));
    }

    //Sort RDD by Key?
    public static void partEight(JavaSparkContext sc) {

        // Carichiamo il file direttamente da un path locale all'interno del progetto.
        // Cambiare il path in caso di necessità.
        JavaRDD<String> initialRDD = sc.textFile("src/main/resources/subtitles/input.txt");
        //JavaRDD<String> initialRDD = sc.textFile("s3://");
        //JavaRDD<String> initialRDD = sc.textFile("hdfs://");

        System.out.println("\nRDD:");
        // L'operazione flatMap utilizza una funzione che concettualmente consuma un valore e produce
        // un numero arbitrario di valori in output. (Consiglio di leggere la documentazione per ulteriori informazioni)
        JavaPairRDD<String, Long> filtered = initialRDD
                .map(element -> element.replaceAll("[^a-zA-Z\\s]", "").toLowerCase())
                .filter(element -> element.trim().length() >= 1)
                .flatMap(element -> Arrays.asList(element.split(" ")).iterator())
                .mapToPair(element -> new Tuple2<>(element, 1L))
                .reduceByKey((value1, value2) -> value1 + value2);

        JavaPairRDD<Long, String> switched = filtered.mapToPair(tuple -> new Tuple2<>(tuple._2, tuple._1));

        // SortByKey ordina per la chiave. A differenza della groupBy questa operazione viene svolta su ogni partizione
        // e successivamente conclusa sul master
        List<Tuple2<Long, String>> results = switched
                .sortByKey(false)
                .take(25);

        results.forEach(element -> System.out.println("KEY: " + element._2 + " - Count: " + element._1));
    }

    //Inner, left and right joins
    public static void partNine(JavaSparkContext sc) {

        // Definisco due piccoli array per testare le operazioni di join
        ArrayList<Tuple2<Integer, Integer>> visitRaw = new ArrayList<>();
        visitRaw.add(new Tuple2<>(4, 18));
        visitRaw.add(new Tuple2<>(6, 4));
        visitRaw.add(new Tuple2<>(10, 9));

        ArrayList<Tuple2<Integer, String>> usersRaw = new ArrayList<>();
        usersRaw.add(new Tuple2<>(1, "John"));
        usersRaw.add(new Tuple2<>(2, "Bob"));
        usersRaw.add(new Tuple2<>(3, "Alan"));
        usersRaw.add(new Tuple2<>(4, "Doris"));
        usersRaw.add(new Tuple2<>(5, "Mary"));
        usersRaw.add(new Tuple2<>(6, "Frank"));

        JavaPairRDD<Integer, Integer> visits = sc.parallelizePairs(visitRaw);
        JavaPairRDD<Integer, String> users = sc.parallelizePairs(usersRaw);

        // InnerJoin
        System.out.println("Inner Join: ");
        JavaPairRDD<Integer, Tuple2<Integer, String>> joinedRDD = visits.join(users);
        joinedRDD.foreach(element -> System.out.println(element._1 +"("+element._2._1+","+element._2._2+")"));

        // LeftOuterJoin
        System.out.println("\nLeft Join: ");
        JavaPairRDD<Integer, Tuple2<Integer, Optional<String>>> leftJoinedRDD = visits.leftOuterJoin(users);
        leftJoinedRDD.foreach(element -> System.out.println(element._1 +"("+element._2._1+","+element._2._2+")"));

        // RightOuterJoin
        System.out.println("\nRight Join: ");
        JavaPairRDD<Integer, Tuple2<Optional<Integer>, String>> rightJoinedRDD = visits.rightOuterJoin(users);
        rightJoinedRDD.foreach(element -> System.out.println(element._1 +"("+element._2._1+","+element._2._2+")"));
    }

}
