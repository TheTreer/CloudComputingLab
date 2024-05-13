import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CsvMapReduce {

    public static class AppMapper
            extends Mapper<Object, Text, IntWritable, DoubleWritable>{

        private final static DoubleWritable height = new DoubleWritable();
        private IntWritable age = new IntWritable();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            Configuration conf = context.getConfiguration();
            String CSV_DELIMITER = conf.get("CSV_DELIMITER");
            int COL_KEY = Integer.valueOf(conf.get("COL_KEY"));
            int COL_VALUE = Integer.valueOf(conf.get("COL_VALUE"));

            String[] tokens = value.toString().split(CSV_DELIMITER);

            if (tokens.length >= COL_KEY && tokens.length >= COL_VALUE) {
                try {
                    age.set(Integer.valueOf(tokens[COL_KEY]));
                    height.set(Double.valueOf(tokens[COL_VALUE]));
                } catch (NumberFormatException e) {
                    System.err.println(e);
                }
            }
            else {
                age.set(-1);
                height.set(0.0);
            }
            context.write(age, height);
        }
    }

    public static class AppReducer
            extends Reducer<IntWritable, DoubleWritable, IntWritable, DoubleWritable> {

        private DoubleWritable result = new DoubleWritable();

        public void reduce(IntWritable key, Iterable<DoubleWritable> values,Context context) throws IOException, InterruptedException {
            double sum = 0.0;
            double total = 0.0;
            for (DoubleWritable value : values) {
                sum += value.get();
                total += 1.0;
            }
            result.set(sum/total);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {

        String INPUT = "";
        String OUTPUT = "";
        String CSV_DELIMITER = ";";
        int COL_KEY = 0;
        int COL_VALUE = 1;

        for (String arg: args){
            String[] pair = arg.split("=");
            switch (pair[0]) {
                case "--input":
                case "-i":
                    INPUT = pair[1];
                    break;
                case "--output":
                case "-o":
                    OUTPUT = pair[1];
                    break;
                case "--delimiter":
                case "-d":
                    CSV_DELIMITER = pair[1];
                    break;
                case "--col-key":
                case "-ck":
                    COL_KEY = Integer.valueOf(pair[1]);
                    break;
                case "--col-value":
                case "-cv":
                    COL_VALUE = Integer.valueOf(pair[1]);
                    break;
                default:
                    System.out.println("[ " + pair[0] + " ] is not a valid option");
                    System.out.println("Usage:");
                    System.out.println("--input | -i: indicates the absolute path of the INPUT file");
                    System.out.println("--output | -o: indicates the absolute path of the OUTPUT file");
                    System.out.println("--delimiter | -d (optional): indicates the CSV delimiter, default is semicolon (;)");
                    System.out.println("--col-key | -ck: indicates which CSV column is the key");
                    System.out.println("--col-value | -cv: indicates which CSV column is the value");
                    System.out.println("Example:");
                    System.out.println("--input=/path/to/file --output=/path/to/another/file -ck=2 -cv=0");
                    return;
            }
        }

        System.out.println("[ INPUT         ] " + INPUT);
        System.out.println("[ OUTPUT        ] " + OUTPUT);
        System.out.println("[ CSV_DELIMITER ] " + CSV_DELIMITER);
        System.out.println("[ COL KEY       ] " + COL_KEY);
        System.out.println("[ COL VALUE     ] " + COL_VALUE);

        Configuration conf = new Configuration();
        conf.set("CSV_DELIMITER", CSV_DELIMITER);
        conf.set("COL_KEY", String.valueOf(COL_KEY));
        conf.set("COL_VALUE", String.valueOf(COL_VALUE));
        Job job = Job.getInstance(conf, "AgeHeight Application");
        job.setJarByClass(CsvMapReduce.class);
        job.setMapperClass(AppMapper.class);
        job.setCombinerClass(AppReducer.class);
        job.setReducerClass(AppReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, new Path(INPUT));
        FileOutputFormat.setOutputPath(job, new Path(OUTPUT));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}