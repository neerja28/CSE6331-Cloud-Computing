/* Author: NEERJA NARAYANAPPA
Student ID: 1001575625 */

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.ReflectionUtils;

/* We are overridng the default Map and reduce functions so we use @Override.
We put the desired logic to complete the task in our map-reduce functions */

class Elem implements Writable {
    int tag;
    int index;
    double value;

    Elem() {
        tag = 0;
        index = 0;
        value = 0.0;
    }

    Elem(int tag, int index, double value) {
        this.tag = tag;
        this.index = index;
        this.value = value;
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        tag = input.readInt();
        index = input.readInt();
        value = input.readDouble();
    }

    @Override
    public void write(DataOutput output) throws IOException {
        output.writeInt(tag);
        output.writeInt(index);
        output.writeDouble(value);
    }
}

class Pair implements WritableComparable<Pair> {

    int i;
    int j;

    Pair() {
        i = 0;
        j = 0;
    }

    Pair(int i, int j) {
        this.i = i;
        this.j = j;
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        i = input.readInt();
        j = input.readInt();
    }

    @Override
    public void write(DataOutput output) throws IOException {
        output.writeInt(i);
        output.writeInt(j);
    }

    @Override
    public int compareTo(Pair o) {

        if(i==o.i)
            return j-o.j;
        else
            return i-o.i;
    }

    public String toString() {
        return i + " " + j + " ";
    }
}

public class Multiply {

    public static class Mapper_M extends Mapper <Object,Text,IntWritable,Elem> {
        @Override
        public void map ( Object key, Text value, Context context ) throws IOException, InterruptedException {
          /* Object,Text,IntWritable,Elem == (input key, input value, ouput key, output value)
          The Mapper_M will give the output as {1,(0,1,-2)}{0,(0,0,5)}...and so on} */
            String lines = value.toString();
            String[] str = lines.split(",");
            int i = Integer.parseInt(str[0]);
            int j = Integer.parseInt(str[1]);
            double v = Double.parseDouble(str[2]);
            /* split line into 3 values: i, j, and v */
            Elem elem = new Elem(0, i, v);
            /* 0 for M, 1 for N */
            IntWritable keyValue = new IntWritable(j);
            /* emit(j,new Elem(0,i,v)) */
            context.write(keyValue, elem);
        }
    }

     public static class Mapper_N extends Mapper<Object,Text,IntWritable,Elem> {
        @Override
        public void map ( Object key, Text value, Context context ) throws IOException, InterruptedException {
          /* Object,Text,IntWritable,Elem == (input key, input value, ouput key, output value)
          The Mapper_N will give the output as {1,(1,0,3)}{0,(1,0,5)}...and so on} */
            String lines = value.toString();
            String[] str = lines.split(",");
            int i = Integer.parseInt(str[0]);
            int j = Integer.parseInt(str[1]);
            double v = Double.parseDouble(str[2]);
            /*split line into 3 values: i, j, and v*/
            Elem elem = new Elem(1, j, v);
            /* 0 for M, 1 for N */
            IntWritable keyValue = new IntWritable(i);
            /*   emit(i,new Elem(1,j,v)) */
            context.write(keyValue, elem);
        }
    }

    public static class Reducer_1 extends Reducer<IntWritable,Elem,Pair,DoubleWritable> {
        @Override
        public void reduce ( IntWritable key, Iterable<Elem> values, Context context ) throws IOException, InterruptedException {
          /* IntWritable,Elem,Pair,DoubleWritable == (input key, input value, ouput key, output value)
          The Reducer_1 will give the output as {1,(0,1,-2)}{0,(0,0,5)..and so on} */
            Vector<Elem> M = new Vector<Elem>();
            Vector<Elem> N = new Vector<Elem>();
            Configuration conf = context.getConfiguration();
            /*   M = all v in values with v.tag==0
                 N = all v in values with v.tag==1 */
            for(Elem Elem : values) {
                Elem tempElem = ReflectionUtils.newInstance(Elem.class, conf);
                ReflectionUtils.copy(conf, Elem, tempElem);
                if (tempElem.tag == 0) {
                    M.add(tempElem);
                } else if(tempElem.tag == 1) {
                    N.add(tempElem);
                }
            }

            for ( Elem a: M){
                for ( Elem b: N)
                    {
                        double val = (a.value * b.value);
                        /* emit(new Pair(a.index,b.index),a.value*b.value)*/
                        context.write(new Pair(a.index,b.index),new DoubleWritable(val));
                    }
            }
        }
    }

    public static class Mapper_MN extends Mapper <Object,Text,Pair,DoubleWritable> {
        @Override
        public void map ( Object key, Text value, Context context ) throws IOException, InterruptedException {
          /* Object,Text,Pair,DoubleWritable == (input key, input value, ouput key, output value)
          The Mapper_MN does nothing its only gives out the (key,value) output */
            String lines = value.toString();
            String[] str = lines.split(" ");
            int i=Integer.parseInt(str[0]);
            int j=Integer.parseInt(str[1]);
            double values=Double.parseDouble(str[2]);
            Pair p = new Pair(i,j);
            DoubleWritable double_value = new DoubleWritable(values);
            /* emit(key,value) */
            context.write(p, double_value);
        }
    }

    public static class Reducer_2 extends Reducer<Pair,DoubleWritable,Pair,DoubleWritable> {
        @Override
        public void reduce(Pair key, Iterable<DoubleWritable> values, Context context)throws IOException, InterruptedException{
          /* Pair,DoubleWritable,Pair,DoubleWritable == (input key, input value, ouput key, output value)
          The Reducer_2 does nothing its only gives out the (key,value) output */
            double res = 0.0;
            /* do the summation */
            for(DoubleWritable val : values) {
                res += val.get();
            }
            context.write(key, new DoubleWritable(res));
            /* emit(pair,m) */
        }
    }

    public static void main ( String[] args ) throws Exception {

        Job job1 = Job.getInstance();
        /* First MapReduce Job with Mapper_M, Mapper_N, Reducer_1
        and its output in the intermediate directory*/
        job1.setJobName("Map_Reduce_Job1");
        job1.setJarByClass(Multiply.class);
        MultipleInputs.addInputPath(job1, new Path(args[0]), TextInputFormat.class, Mapper_M.class);
        MultipleInputs.addInputPath(job1, new Path(args[1]), TextInputFormat.class, Mapper_N.class);
        /* Using Multiple Inputs to pass args to the Mapper classes */
        job1.setReducerClass(Reducer_1.class);
        FileOutputFormat.setOutputPath(job1, new Path(args[2]));
        /* Store the output in the intermediate folder */
        job1.setMapOutputKeyClass(IntWritable.class);
        job1.setMapOutputValueClass(Elem.class);
        /* {key,value} = {IntWritable,Elem} */
        job1.setOutputKeyClass(Pair.class);
        job1.setOutputValueClass(DoubleWritable.class);
        /* {key,value} = {Pair,DoubleWritable} */
        job1.setOutputFormatClass(TextOutputFormat.class);
        /* This output in Text format is the input for Mapper_MN */
        job1.waitForCompletion(true);


        Job job2 = Job.getInstance();
        /* Second MapReduce Job with Mapper_MN, Mapper_N, Reducer_2
        and its output in the output directory*/
        job2.setJobName("Map_Reduce_Job2");
        job2.setJarByClass(Multiply.class);
        job2.setMapperClass(Mapper_MN.class);
        job2.setReducerClass(Reducer_2.class);
        FileInputFormat.setInputPaths(job2, new Path(args[2]));
        /* Input will be fetched from the Intermediate directory */
        FileOutputFormat.setOutputPath(job2, new Path(args[3]));
        /* Output will be in the output directory */
        job2.setInputFormatClass(TextInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);
        /* Input and output is of text format*/
        job2.setMapOutputKeyClass(Pair.class);
        job2.setMapOutputValueClass(DoubleWritable.class);
        /* {key,value} = {Pair,DoubleWritable} */
        job2.setOutputKeyClass(Pair.class);
        job2.setOutputValueClass(DoubleWritable.class);
        /* {key,value} = {Pair,DoubleWritable} */
        job2.waitForCompletion(true);
    }
}
