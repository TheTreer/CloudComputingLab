import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;


public class AthleteAge {

    // 第一个MapReduce用来提取出数据，并且计数
    public static class Mapper1 extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
        private Text word = new Text("");

        public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
            String line = value.toString();
            String[] csvLine = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
            String id = csvLine[0];
            if ("\"ID\"".equals(id))
                return;
            int year = Integer.parseInt(csvLine[9]);
            if (year < 1996 || "NA".equals(csvLine[3]))
                return;
            int age = Integer.parseInt(csvLine[3]);
            output.collect(word, new IntWritable(age));
        }
    }

    public static class Reducer1 extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
            int sum = 0 , count = 0, min = 200, max = 0, age = 0;
            while (values.hasNext()) {
                age = values.next().get();
                ++count;
                sum += age;
                if (age > max)
                    max = age;
                if (age < min)
                    min = age;
            }
            int avg = sum / count;
            output.collect(new Text("min"), new IntWritable(min));
            output.collect(new Text("max"), new IntWritable(max));
            output.collect(new Text("avg"), new IntWritable(avg));
        }
    }


    public static void main(String[] args) throws Exception {
        // job1，负责count
        JobConf job1 = new JobConf(AthleteAge.class);
        job1.setJobName("Test2-job1-count");

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(IntWritable.class);

        job1.setMapperClass(AthleteAge.Mapper1.class);
//        job1.setCombinerClass(AthleteStat.Reducer1.class);
        job1.setReducerClass(AthleteAge.Reducer1.class);

        job1.setInputFormat(TextInputFormat.class);
        job1.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));

        JobClient.runJob(job1);
    }
}