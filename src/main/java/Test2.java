import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;


public class Test2 {

    // 第一个MapReduce用来提取出数据，并且计数
    public static class Mapper1 extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
            String line = value.toString();
            String[] csvLine = line.split(",");
            word.set(csvLine[1]);
            output.collect(word, one);
        }
    }

    public static class Reducer1 extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
            int sum = 0;
            while (values.hasNext()) {
                sum += values.next().get();
            }
            output.collect(key, new IntWritable(sum));
        }
    }

    //第二个MapReduce用来倒序输出
    public static class Mapper2 extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, Text> {
        private IntWritable num = new IntWritable(0);
        private Text word = new Text();

        public void map(LongWritable key, Text value, OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {
            String line = value.toString();
            String[] pair = line.split("\t");
            num.set(Integer.parseInt(pair[1]));
            word.set(pair[0]);
            output.collect(num, word);
        }
    }

    public static class Reducer2 extends MapReduceBase implements Reducer<IntWritable, Text, Text, IntWritable> {
        public void reduce(IntWritable key, Iterator<Text> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
            while (values.hasNext()) {
                output.collect(values.next(), key);
            }
        }
    }

    public static class DescKeyComparator extends WritableComparator {
        protected DescKeyComparator() {
            super(IntWritable.class, true);
        }

        @Override
        public int compare(WritableComparable key1, WritableComparable key2) {
            return -1 * key1.compareTo(key2);
        }
    }

    public static void main(String[] args) throws Exception {
        // job1，负责count
        JobConf job1 = new JobConf(MostTimeAthlete.class);
        job1.setJobName("Test2-job1-count");

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);

        job1.setMapperClass(MostTimeAthlete.Mapper1.class);
        job1.setCombinerClass(MostTimeAthlete.Reducer1.class);
        job1.setReducerClass(MostTimeAthlete.Reducer1.class);

        job1.setInputFormat(TextInputFormat.class);
        job1.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));

        JobClient.runJob(job1);


        // job2，负责sort
        JobConf job2 = new JobConf(MostTimeAthlete.class);
        job2.setJobName("Test2-job2-sort");

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);
        job2.setMapOutputKeyClass(IntWritable.class);
        job2.setMapOutputValueClass(Text.class);

        job2.setMapperClass(MostTimeAthlete.Mapper2.class);
//        job2.setCombinerClass(Test2.Reducer2.class);
        job2.setReducerClass(MostTimeAthlete.Reducer2.class);

        job2.setInputFormat(TextInputFormat.class);
        job2.setOutputFormat(TextOutputFormat.class);

        // 设置排序的函数，如果不设置此项，那么将不会倒序输出
        job2.setOutputKeyComparatorClass(DescKeyComparator.class);

        // 设置路径，job1输出变为job2输入
        FileInputFormat.setInputPaths(job2, new Path(args[1]));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));

        JobClient.runJob(job2);

    }
}