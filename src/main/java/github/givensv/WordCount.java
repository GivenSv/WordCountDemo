package github.givensv;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.StringTokenizer;

public class WordCount {

    public static class MyMapper extends Mapper<Object, Text, Text, IntWritable>{
        private final static IntWritable one  = new IntWritable(1);
        private Text word =  new Text();

        public void map(Object key, @NotNull Text value, Context context) throws IOException,InterruptedException {

            StringTokenizer itr = new StringTokenizer(value.toString());
            while(itr.hasMoreTokens()){
                word.set(itr.nextToken());
                context.write(word, one);
            }

        }





    }

    public static class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
        private IntWritable result = new IntWritable();

        public void reduce(Text key, @NotNull Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for(IntWritable val : values){
                sum += val.get();

            }
            result.set(sum);
            context.write(key, result);
        }



    }

    public static class MyMapper2 extends Mapper<Object, Text, Text, IntWritable>{
        private final static IntWritable one = new IntWritable(1);

        private Text word = new Text("The num of words:");

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            context.write(word, one);
        }


    }

    public static void main(String @NotNull [] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration configuration = new Configuration();

        Job job  = Job.getInstance(configuration, "word count");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        FileSystem fileSystem = FileSystem.get(configuration);
        fileSystem.delete(new Path(args[1]), true);
        job.waitForCompletion(true);

        Job job2 = Job.getInstance(configuration, "word count-2");
        job.setJarByClass(WordCount.class);
        job2.setMapperClass(WordCount.MyMapper2.class);
        job2.setReducerClass(WordCount.MyReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job2, new Path(args[1]));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));
        fileSystem.delete(new Path(args[2]), true);
        boolean result = job2.waitForCompletion(true);

        System.exit(result?0:1);
    }
}