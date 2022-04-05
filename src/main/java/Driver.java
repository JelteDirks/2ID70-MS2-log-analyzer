import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Driver {

    public static JavaSparkContext getContext() {
        SparkConf conf = new SparkConf()
                .setAppName("DA")
                .setMaster("local[*]");

        JavaSparkContext sparkContext = new JavaSparkContext(conf);
        sparkContext.setLogLevel("warn");

        return sparkContext;
    }

    public static void driver() {
        JavaSparkContext sparkContext = getContext();

        JavaRDD<String> logs = sparkContext.textFile("/Users/jeltedirks/IdeaProjects/spark-playground/logs/submission-*.spark-submit.log", 16);

        logs.flatMap(line -> Arrays.stream(line.split("\\s+|\\W|\\d+")).iterator())
                .filter(word -> word.length() > 1)
                .mapToPair(word -> {
                    return new Tuple2<>(word, 1);
                })
                .aggregateByKey(0,
                        Integer::sum,
                        Integer::sum)
                .sortByKey();

        JavaRDD<Long> worker0BytesList = logs.filter(line -> line.contains("worker-0"))
                .filter(Driver::addRDDToMemoryFilter)
                .map(Driver::getSize)
                .map(Driver::stringSizeToBytes);

        JavaRDD<Long> worker1BytesList = logs.filter(line -> line.contains("worker-1"))
                .filter(Driver::addRDDToMemoryFilter)
                .map(Driver::getSize)
                .map(Driver::stringSizeToBytes);

        System.out.println(Driver.bytesToGibibytes(worker0BytesList.reduce(Long::sum)));
        System.out.println(Driver.bytesToGibibytes(worker1BytesList.reduce(Long::sum)));

    }

    public static String bytesToGibibytes(long bytes) {
        double sizeGiB = bytes / Math.pow(1024, 3);
        return sizeGiB + " GiB";
    }

    public static long stringSizeToBytes(String line) {
        int base = 1024;
        int power = 0;

        if (line.contains("KiB")) {
            power = 1;
        } else if (line.contains("MiB")) {
            power = 2;
        } else if (line.contains("GiB")) {
            power = 3;
        }

        String[] s = line.split("\\s");
        double number = Double.parseDouble(s[0]);

        return (long) (Math.pow(base, power) * number);
    }

    public static void main(String[] args) {
        driver();
    }

    private static Boolean addRDDToMemoryFilter(String line) {
        return line.matches("^.*Added rdd_\\d+_\\d+ in memory on.*$");
    }

    private static String getSize(String line) {
        Pattern p = Pattern.compile(".*size: (.*),.*");
        Matcher m = p.matcher(line);

        if (m.matches()) {
            return m.group(1);
        }

        return "";
    }
}