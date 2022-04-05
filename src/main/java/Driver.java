import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.time.Duration;
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

    private static void averageTaskRunningTime() {
        JavaRDD<String> worker0 = getWorker0();

        JavaRDD<String> x1 = worker0.filter(Driver::keepFinishedTask);
        JavaRDD<Long> x2 = x1.map(Driver::getRunningTimeOfTask)
                .filter(l -> l != -1)
                .cache();
        long finishedTasks = x2.count();
        long x3 = x2.reduce(Long::sum);
        Duration runningTime0 = Duration.ofMillis(x3);
        System.out.println("Worker 0 has worked for " +
                runningTime0.toMinutesPart() + " minutes and " +
                runningTime0.toSecondsPart() + " seconds");
    }

    private static long getRunningTimeOfTask(String line) {
        Pattern p = Pattern.compile(".*in (\\d+) m?s on.*");
        Matcher m = p.matcher(line);
        if (m.matches()) {
            return Long.parseLong(m.group(1));
        }
        return -1;
    }

    private static boolean keepFinishedTask(String line) {
        Pattern p = Pattern.compile(".*TaskSetManager: Finished task.*");
        Matcher m = p.matcher(line);
        return m.matches();
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
        averageTaskRunningTime();
    }

}