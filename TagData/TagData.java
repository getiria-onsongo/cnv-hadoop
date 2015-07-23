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

public class TagData {
    private static String tagLabel;
    
    public static class Map extends Mapper<LongWritable, Text, Text, Text> {
        private Text chrPos = new Text();
        private Text coverageLabel = new Text();
        
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String delimiter = "\\t";
            String[] strArray = line.split(delimiter);

            chrPos.set(strArray[0] + ":" + strArray[1]);
            coverageLabel.set(strArray[2] + ":" + tagLabel);
            context.write(chrPos,coverageLabel);
        }
    }
    
    
    public static void main(String[] args) throws Exception {
        
        if (args.length != 3) {
            System.err.println("Usage: TagData <input file> <output path> <tag label>");
            System.exit(-1);
        }
        
        Job job = new Job();
        job.setJarByClass(TagData.class);
        job.setJobName("TagData");
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        tagLabel = args[2];
        
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        job.setMapperClass(Map.class);
        job.setNumReduceTasks(0);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
