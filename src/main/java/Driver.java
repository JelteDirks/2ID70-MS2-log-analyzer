import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.time.Duration;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Driver {
    private static void workerMemoryUsage(JavaRDD<String> workerLogs, String id) {
        JavaRDD<Long> worker0BytesList = workerLogs
                .filter(Driver::addRDDToMemoryFilter)
                .map(Driver::getSize)
                .map(Driver::stringSizeToBytes);

        System.out.println(id + " has added a total of " + Driver.bytesToGibibytes(worker0BytesList.reduce(Long::sum)) + " to memory");
    }

    private static void runningTimeStats(JavaRDD<String> workerLogs, String id) {
        JavaRDD<Long> x1 = workerLogs.filter(Driver::keepFinishedTask)
                .map(Driver::getRunningTimeOfTask)
                .filter(l -> l != -1)
                .cache();
        long finishedTasks = x1.count();
        long runningTimeMillis = x1.reduce(Long::sum);
        Duration runningTime = Duration.ofMillis(runningTimeMillis);
        int avg = Math.toIntExact(runningTimeMillis / finishedTasks);

        System.out.println(id + " has worked for " +
                runningTime.toMinutesPart() + " minutes and " +
                runningTime.toSecondsPart() + " seconds");

        System.out.println(id + " took an average of " + avg + " ms");
    }

    private static JavaRDD<String> LOGS = null;

    private static JavaRDD<String> getLogs() {
        if (LOGS == null) {
            LOGS = sparkContext.textFile("/Users/jeltedirks/IdeaProjects/spark-playground/logs/submission-*.spark-submit.log", PARTITIONS);
        }
        return LOGS;
    }

    private static JavaRDD<String> getWorker0() {
        JavaRDD<String> logs = getLogs();
        return logs.filter(line -> line.contains("worker-0"));
    }

    private static JavaRDD<String> getWorker1() {
        JavaRDD<String> logs = getLogs();
        return logs.filter(line -> line.contains("worker-1"));
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

    private static final int PARTITIONS = 16;
    private static final SparkConf conf = new SparkConf()
            .setAppName("DA")
            .setMaster("local[*]");

    private static final JavaSparkContext sparkContext = new JavaSparkContext(conf);

    public static void main(String[] args) {
        sparkContext.setLogLevel("WARN");
        workerMemoryUsage(getWorker0(), "Worker 0");
        workerMemoryUsage(getWorker1(), "Worker 1");
        runningTimeStats(getWorker0(), "Worker 0");
        runningTimeStats(getWorker1(), "Worker 1");
    }

}