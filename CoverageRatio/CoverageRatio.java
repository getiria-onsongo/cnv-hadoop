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

public class CoverageRatio {
    
    public static class Map extends Mapper<LongWritable, Text, Text, Text> {
        private Text chrPos = new Text();
        private Text coverageLabel = new Text();
        
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String delimiter = "\\t";
            String[] strArray = line.split(delimiter);
            
            chrPos.set(strArray[0]);
            coverageLabel.set(strArray[1]);
            context.write(chrPos,coverageLabel);
        }
    }
  public static class Reduce extends Reducer<Text, Text, Text, FloatWritable> {
        private FloatWritable ratio = new FloatWritable();
        
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Text output = new Text();
            for (Text value : values) {
                if(output.toString().length() == 0 && value.toString().length() > 0){
                    output.set(value.toString());
                }else{
                    output.set(output.toString() + "-" + value.toString());
                }
            }
            
            float ratio;
            int numerator_coverage;
            int denominator_coverage;
            
            String line = output.toString();
            String dash_delimiter = "-";
            String[] strArray = line.split(dash_delimiter);
            
            if(strArray.length < 2){
                // Do nothing: This is not a valid chr:pos with both numerator and coverage data
            }else{
                String colon_delimiter = ":";
                String numerator = "n";
                String[] coverageArrayOne = strArray[0].split(colon_delimiter);
                String[] coverageArrayTwo = strArray[1].split(colon_delimiter);
                
                if(coverageArrayOne[1].compareToIgnoreCase(numerator) == 0){
                    numerator_coverage = Integer.parseInt(coverageArrayOne[0]);
                    denominator_coverage = Integer.parseInt(coverageArrayTwo[0]);
                }else{
                    denominator_coverage = Integer.parseInt(coverageArrayOne[0]);
                    numerator_coverage = Integer.parseInt(coverageArrayTwo[0]);
                }
                if(numerator_coverage > 0 && denominator_coverage > 0){
                    ratio = ((float)numerator_coverage/ (float)denominator_coverage);
                    context.write(key,new FloatWritable(ratio));
                }
            }
            
        }
    }

    public static void main(String[] args) throws Exception {
        
        if (args.length != 3) {
            System.err.println("Usage: CoverageRatio <input file1> <input file2> <output path>");
            System.exit(-1);
        }
        
        Job job = new Job();
        job.setJarByClass(CoverageRatio.class);
        job.setJobName("CoverageRatio");
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);
        
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
