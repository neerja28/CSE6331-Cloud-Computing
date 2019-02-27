/* Author: NEERJA NARAYANAPPA
Student ID: 1001575625 */

import java.io.IOException;
import java.util.Scanner;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

/* WE are overriidng the default Map and reduce functions sso use use @Override
We put the desired logic to complete the task in our map-reduce functions */

public class Graph {
    public static class Mapper1 extends Mapper<Object,Text,IntWritable,IntWritable> {
    /* Object,Text,IntWritable,IntWritable == (input key, input value, ouput key, output value)
    The first map-reduce will give the output as * nodes have * neighbours */

        @Override
        public void map ( Object key, Text value, Context context) throws IOException, InterruptedException {
            Scanner s = new Scanner(value.toString()).useDelimiter(",");
            int x = s.nextInt();
            int y = s.nextInt();
            context.write(new IntWritable(x),new IntWritable(y));
            /* We use Context class and write method to collect the put from Mapper function */
            s.close();
        }
    }

    public static class Reducer1 extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable> {
    /* IntWritable,IntWritable,IntWritable,IntWritable == (input key, input value, ouput key, output value) */
        @Override
        public void reduce ( IntWritable key, Iterable<IntWritable> values, Context context )
                           throws IOException, InterruptedException {
              int count = 0;
            for (IntWritable v: values) {
                count++;
            };
            context.write(key,new IntWritable(count));

        }
    }

    public static class Mapper2 extends Mapper<IntWritable,IntWritable,IntWritable,IntWritable> {
    /* IntWritable,IntWritable,IntWritable,IntWritable == (input key, input value, ouput key, output value) */
        @Override
        public void map ( IntWritable key, IntWritable value, Context context) throws IOException, InterruptedException {
            Scanner s = new Scanner(value.toString()).useDelimiter(",");
            int neighbours = s.nextInt();
            context.write(new IntWritable(neighbours),new IntWritable(1));
            s.close();
        }
    }

    public static class Reducer2 extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable> {
    /* IntWritable,IntWritable,IntWritable,IntWritable == (input key, input value, ouput key, output value) */
        @Override
        public void reduce ( IntWritable key, Iterable<IntWritable> values, Context context ) throws IOException, InterruptedException {
            int count = 0;
            for (IntWritable v: values) {
                count++;
            };
            context.write(key,new IntWritable(count));
        }
    }

    public static void main ( String[] args ) throws Exception {
        String tempDir ="/output/temp";

        Job job1 = Job.getInstance();
        job1.setJobName("Map-Reduce-Job1");
        job1.setJarByClass(Graph.class);

        job1.setInputFormatClass(TextInputFormat.class);
        /* We are using the default file input format which is the text file input format
        The output of this line would be key value pairs of the input file */
        job1.setOutputFormatClass(SequenceFileOutputFormat.class);
        /*The output of the job1 would be key values pairs as binary objects*/

        job1.setOutputKeyClass(IntWritable.class);
        job1.setOutputValueClass(IntWritable.class);

        job1.setMapOutputKeyClass(IntWritable.class);
        job1.setMapOutputValueClass(IntWritable.class);

        job1.setMapperClass(Mapper1.class);
        job1.setReducerClass(Reducer1.class);


        FileInputFormat.setInputPaths(job1,new Path(args[0]));
        FileOutputFormat.setOutputPath(job1,new Path("temp"));

        boolean success=job1.waitForCompletion(true);


        if(success){
        Job job2 = Job.getInstance();
        job2.setJobName("Map-Reduce-Job2");
        job2.setJarByClass(Graph.class);

        job2.setOutputKeyClass(IntWritable.class);
        job2.setOutputValueClass(IntWritable.class);

        job2.setMapOutputKeyClass(IntWritable.class);
        job2.setMapOutputValueClass(IntWritable.class);

        job2.setMapperClass(Mapper2.class);
        job2.setReducerClass(Reducer2.class);

        job2.setInputFormatClass(SequenceFileInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job2,new Path("temp"));
        FileOutputFormat.setOutputPath(job2,new Path(args[1]));
        job2.waitForCompletion(true);
  }
 }
}
