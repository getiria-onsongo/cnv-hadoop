import java.io.*;
import java.io.IOException;
import java.util.*;
import java.net.URI;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


public class WithinRatio {
    
    private static final Log LOG = LogFactory.getLog(WithinRatio.class);
    private static String[][] medianCoverage = new String[3][2];
    
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
        private Text newKey = new Text();
      
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            
            String filename = "reference_pileup_path";
            String line = null;
            int lineNumber = 0;
            
            try {
                // FileReader reads text files in the default encoding.
                FileReader fileReader = new FileReader(filename);
                // Always wrap FileReader in BufferedReader.
                BufferedReader bufferedReader = new BufferedReader(fileReader);
                while((line = bufferedReader.readLine()) != null) {
                    String lineHolder = line.toString();
                    String delimiter = ",";
                    String [] strArray = lineHolder.split(delimiter);
                    
                    medianCoverage[lineNumber][0] = strArray[0];
                    medianCoverage[lineNumber][1] = strArray[1];
                    lineNumber++;
                    
                }
                // Always close files.
                bufferedReader.close();
            }
            catch(FileNotFoundException ex) {
                LOG.fatal("Unable to open file '" + filename + "'");
            }
            catch(IOException ex) {
                LOG.fatal("Error reading file '" + filename + "'");
                // Or we could just do this:
                // ex.printStackTrace();
            }
            
            for (IntWritable value : values) {
                float ratio;
                for(int i=0; i<3; i++){
                    newKey.set(medianCoverage[i][0] + "," + key.toString());
                    ratio = ((float)value.get() / (float) Integer.parseInt(medianCoverage[i][1]));
                    context.write(newKey,new FloatWritable(ratio));
                }
            }
            
        }
      
    }
    
    
    public static void main(String[] args) throws Exception {
        
        if (args.length != 3) {
            System.err.println("Usage: WithinRatio <input file> <output path> <reference_pileup path>");
            System.exit(-1);
        }
        
        Job job = new Job();
        job.setJarByClass(WithinRatio.class);
        job.setJobName("WithinRatio");
        
        Configuration conf = job.getConfiguration();
        
        
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        String reference_pileup_path = args[2];
        
        StringBuilder referencePileupLink= new StringBuilder();
        referencePileupLink.append(reference_pileup_path);
        referencePileupLink.append("#reference_pileup_path");
        String link = referencePileupLink.toString();
        
        LOG.info(link);
        LOG.info("Put reference median coverage to distributed cache");
        DistributedCache.addCacheFile(new URI(link),conf);
        
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
