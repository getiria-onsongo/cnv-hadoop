import java.io.*;
import java.io.IOException;
import java.util.*;
import java.net.URI;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;

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
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
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


public class Test extends Configured implements Tool{
    
    private static Logger theLogger = Logger.getLogger(Test.class.getName());
    
    public static class TestMap extends Mapper<LongWritable, Text, Text, Text> {
        
        private Text jobID = new Text();
        private Text inputLineValue = new Text();
        
    
        protected void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException{
            int id = context.getTaskAttemptID().getTaskID().getId();
            jobID.set(String.valueOf(id));
	    
            ProcessBuilder pb;
            Process process;
            File errorFile;
            File outputFile;
            Configuration mapper_conf = context.getConfiguration();
            String mapper_bwa_db = mapper_conf.get("bwa_db");
            
            pb = new ProcessBuilder("hadoop","fs","-get",mapper_bwa_db,"bwa.tar.gz");
            pb.inheritIO();
            theLogger.info("Command executed: " + pb.command());
            process = pb.start();
            process.waitFor();

	    /**            
            pb = new ProcessBuilder("tar","zxvf","bwa.tar.gz");
            pb.inheritIO();
            theLogger.info("Command executed: " + pb.command());
            process = pb.start();
            process.waitFor();
            
            pb = new ProcessBuilder("/opt/hadoop-2.7.1/bin/hadoop","fs","-put","bwa/hg19_canonical.fa.sa","/groups/riss/onson001/hg19_canonical.fa.sa");
            pb.inheritIO();
            theLogger.info("Command executed: " + pb.command());
            process = pb.start();
            process.waitFor();
            **/
            inputLineValue.set("Test");
            context.write(jobID,inputLineValue);
            
        } // end of map method
	
    }// end of mapper class

    @Override
    public int run(String[] args) throws Exception {
      
        
        if (args.length != 3) {
            System.err.println("Usage: Test <input file> <output path> <bwa_db>");
            System.exit(-1);
        }
        
        Configuration conf = new Configuration();
        args = new GenericOptionsParser(conf, args).getRemainingArgs();
        // One line per mapper
        conf.setInt(NLineInputFormat.LINES_PER_MAP, 1);
        String inputPath = args[0];
        String output = args[1];
        
        conf.set("bwa_db",args[2]);
	
        Job job = new Job(conf, "Test");
        job.setJarByClass(TestMap.class);
        
        job.setInputFormatClass(NLineInputFormat.class);
        job.setMapperClass(TestMap.class);

        job.setNumReduceTasks(0);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, new Path(inputPath));
        Path outPath = new Path(output);
        FileOutputFormat.setOutputPath(job, outPath);
        outPath.getFileSystem(conf).delete(outPath, true);
        
        job.waitForCompletion(true);
        
        return (job.waitForCompletion(true) ? 0 : 1);
    }
    
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Test(), args);
        System.exit(exitCode);
    }
}
