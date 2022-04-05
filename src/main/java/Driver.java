import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Driver {

    private static final int PARTITIONS = 16;
    private static final SparkConf conf = new SparkConf()
            .setAppName("DA")
            .setMaster("local[*]");

    private static final JavaSparkContext sparkContext = new JavaSparkContext(conf);

    private static JavaRDD<String> getLogs() {
        return sparkContext.textFile("/Users/jeltedirks/IdeaProjects/spark-playground/logs/submission-*.spark-submit.log", PARTITIONS);
    }

    private static JavaRDD<String> getWorker0() {
        JavaRDD<String> logs = getLogs();
        return logs.filter(line -> line.contains("worker-0"));
    }

    private static JavaRDD<String> getWorker1() {
        JavaRDD<String> logs = getLogs();
        return logs.filter(line -> line.contains("worker-1"));
    }

    private static void workerMemoryUsage() {
        JavaRDD<String> logs = getLogs();

        JavaRDD<Long> worker0BytesList = getWorker0()
                .filter(Driver::addRDDToMemoryFilter)
                .map(Driver::getSize)
                .map(Driver::stringSizeToBytes);

        JavaRDD<Long> worker1BytesList = getWorker1()
                .filter(Driver::addRDDToMemoryFilter)
                .map(Driver::getSize)
                .map(Driver::stringSizeToBytes);

        System.out.println("Worker 0 has added a total of " + Driver.bytesToGibibytes(worker0BytesList.reduce(Long::sum)) + " to memory");
        System.out.println("Worker 1 has added a total of " + Driver.bytesToGibibytes(worker1BytesList.reduce(Long::sum)) + " to memory");
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

    public static void main(String[] args) {
        sparkContext.setLogLevel("WARN");
        workerMemoryUsage();
    }

}