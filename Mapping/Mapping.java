import java.io.*;
import java.io.IOException;
import java.util.*;
import java.net.URI;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.LogManager;


public class NormalizeRatio extends Configured implements Tool{
    
    private static Logger theLogger = Logger.getLogger(NormalizeRatio.class.getName());
    
    public static class NormalizeRatioMap extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        
        String record;
        private Text chrPos = new Text();
        String filename = "avgRatioLog2_path";
        String line = null;
        Double avg_ratio;
        Double coverage_ratio;
        Double ratioLog2;
        Double norm_ratioLog2;
        
        protected void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException{
	    
            try {
                // FileReader reads text files in the default encoding.
                FileReader fileReader = new FileReader(filename);
                // Always wrap FileReader in BufferedReader.
                BufferedReader bufferedReader = new BufferedReader(fileReader);
                line = bufferedReader.readLine();
                String lineHolder = line.toString();
                String delimiter = "\\s+";
                String [] strArray = lineHolder.split(delimiter);
                avg_ratio = Double.parseDouble(strArray[1]);

                // Always close files.
              bufferedReader.close();
           }
            catch(FileNotFoundException ex) {
                 theLogger.info("Unable to open file '" + filename + "'");
            }
            catch(IOException ex) {
                 theLogger.info("Error reading file '" + filename + "'");
                // Or we could just do this:
                // ex.printStackTrace();
            }
            record = value.toString();
            String[] fields = record.split("\\s+");
            if(fields.length > 5){
                chrPos.set(fields[2] + ":" + fields[3]);
                coverage_ratio = Double.parseDouble(fields[4]);
                /**
                 One can easily calculate the logarithm of any base using the following simple equation:
                 log[x] base c = log[x] base k / log[c] base k
		   
                 We are using the above expression to convert ratios to base 2. To see why we need to
                 work in base 2, consider 2 ratio (0.5 and 2.0). Computing direct ratio of the coverage
                 will give 1.25 despite the two ratios being inverses of each other. We need to work in
                 base two why decrease and increase is symmetrical around 0 (ratio = 1).
                 **/
                
                ratioLog2 = new Double(Math.log(coverage_ratio) / Math.log(2));
                norm_ratioLog2 = new Double(ratioLog2 - avg_ratio);
                //theLogger.info("\n \n ------------ HERE HERE!");
                //theLogger.info("------------ chrPos: \t"+chrPos);
                //theLogger.info("------------ coverage_ratio: \t"+coverage_ratio);
                //theLogger.info("------------ avg_ratio: \t"+avg_ratio);
                //theLogger.info("------------ ratioLog2: \t"+ratioLog2);
                //theLogger.info("------------ norm_ratioLog2: \t"+norm_ratioLog2);
                //theLogger.info("------------ Math.pow(2,norm_ratioLog2): \t"+Math.pow(2,norm_ratioLog2));
                 context.write(chrPos,new DoubleWritable(Math.pow(2,norm_ratioLog2)));
                //context.write(chrPos,new DoubleWritable(1));
		
            } // end if(fields.length > 5)
            
         
	} // end of map method
	
    }// end of mapper class

    @Override
    public int run(String[] args) throws Exception {
        
        if (args.length != 3) {
            System.err.println("Usage: NormalizeRatio <input file> <avgRatioLog2> <output path> ");
            System.exit(-1);
        }
        
        Configuration conf = new Configuration();
        args = new GenericOptionsParser(conf, args).getRemainingArgs();
        String input = args[0];
        String avgRatioLog2_path = args[1];
        String output = args[2];
        
        StringBuilder referencePileupLink= new StringBuilder();
        referencePileupLink.append(avgRatioLog2_path);
        referencePileupLink.append("#avgRatioLog2_path");
        String link = referencePileupLink.toString();
        DistributedCache.addCacheFile(new URI(link),conf);
        
        Job job = new Job(conf, "NormalizeRatio");
        job.setJarByClass(NormalizeRatioMap.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(NormalizeRatioMap.class);

        job.setNumReduceTasks(0);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        FileInputFormat.setInputPaths(job, new Path(input));
        Path outPath = new Path(output);
        FileOutputFormat.setOutputPath(job, outPath);
        outPath.getFileSystem(conf).delete(outPath, true);
        
        job.waitForCompletion(true);
        return (job.waitForCompletion(true) ? 0 : 1);
    }
    
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new NormalizeRatio(), args);
        System.exit(exitCode);
    }
}
