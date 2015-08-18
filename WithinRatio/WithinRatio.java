import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class WithinRatio {
    
    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
        private Text chrPos = new Text();
        
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String delimiter = "\\t";
            String[] strArray = line.split(delimiter);
            
            chrPos.set(strArray[0] + ":" + strArray[1]);
            
            context.write(chrPos,new IntWritable(Integer.parseInt(strArray[2])));
        }
    }
  public static class Reduce extends Reducer<Text, IntWritable, Text, FloatWritable> {
        private FloatWritable ratio = new FloatWritable();
        
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            for (IntWritable value : values) {
                float ratio;
                
                ratio = ((float)value.get() / (float)2);
                context.write(key,new FloatWritable(ratio));
            }
            
        }
    }

    public static void main(String[] args) throws Exception {
        
        if (args.length != 2) {
            System.err.println("Usage: WithinRatio <input file1> <output path>");
            System.exit(-1);
        }
        
        Job job = new Job();
        job.setJarByClass(WithinRatio.class);
        job.setJobName("WithinRatio");
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);
        
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
