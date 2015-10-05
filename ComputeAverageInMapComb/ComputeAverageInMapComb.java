import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
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


// Modified from: https://dzone.com/articles/mapper-combiner-program

/**
 Advantages of in-mapper combiner over traditional combiner:
 
 When a mapper with a traditional combiner (the mini-reducer) emits the key-value pair, they are collected 
 in the memory buffer and then the combiner aggregates a batch of these key-value pairs before sending them 
 to the reducer. The drawbacks of this approach are:

 1) The execution of combiner is not guaranteed; so MapReduce jobs cannot depend on the combiner execution.
 
 2) Hadoop may store the key-value pairs in local filesystem, and run the combiner later which will cause 
 expensive disk IO.
 
 3) A combiner only combines data in the same buffer. Thus, we may still generate a lot of network traffic 
 during the shuffle phase even if most of the keys from a single mapper are the same. To see this, consider 
 the word count example, assuming that buffer size is 3, and we have <key, value> = <Stanford, 3>, 
 <Berkeley, 1>, <Stanford, 7>, <Berkeley, 7>, and <Stanford, 2> emitted from one mapper. The first three 
 items will be in one buffer, and last two will be in the the other buffer; as a result, the combiner will 
 emit <Stanford, 10>, <Berkeley, 1>, <Berkeley, 7>, <Stanford, 2>. If we use in-mapper combiner, we will 
 get <Stanford, 12>, <Berkeley, 8>.
 **/

public class ComputeAverageInMapComb extends Configured implements Tool{
    
    private static Logger theLogger = Logger.getLogger(ComputeAverageInMapComb.class.getName());
    
    public static class ComputeAverageInMapCombMap extends Mapper<LongWritable, Text, DoubleWritable, DoublePair> {
        
        String record;
        Map partial_sum = new HashMap<Double, Double>();
        Map record_count = new HashMap<Double, Double>();
        
        protected void map(LongWritable key, Text value, Mapper.Context context) {
            record = value.toString();
            String[] fields = record.split("\\s+");
            
            if(fields.length > 5){
                Double s_id = new Double(1);
                Double coverage_ratio = Double.parseDouble(fields[4]);
                /**
                One can easily calculate the logarithm of any base using the following simple equation:
                 log[x] base c = log[x] base k / log[c] base k
                 
                 We are using the above expression to convert ratios to base 2. To see why we need to 
                 work in base 2, consider 2 ratio (0.5 and 2.0). Computing direct ratio of the coverage 
                 will give 1.25 despite the two ratios being inverses of each other. We need to work in
                 base two why decrease and increase is symmetrical around 0 (ratio = 1). 
                 
                 **/
                
                Double ratioLog2 = new Double(Math.log(coverage_ratio) / Math.log(2));
                Double lowerBound = new Double(Math.log(0.5) / Math.log(2));
                Double upperBound = new Double(Math.log(2.0) / Math.log(2));
            
                if((lowerBound <= ratioLog2) && (ratioLog2 <= upperBound)){
                    if (partial_sum.containsKey(s_id)) {
                        Double sum = (Double) partial_sum.get(s_id) + ratioLog2;
                        partial_sum.put(s_id, sum);
                    } else {
                        partial_sum.put(s_id, ratioLog2);
                    }
            
                    if (record_count.containsKey(s_id)) {
                        Double count = (Double) record_count.get(s_id) + 1;
                        record_count.put(s_id, count);
                    } else {
                        record_count.put(s_id, new Double(1));
                    }
                } //end if((lowerBound <= ratioLog2) && (ratioLog2 <= upperBound))
            } // end if(fields.length > 5)
            
        } // end of map method
        
        protected void cleanup(Context context) throws IOException, InterruptedException {
            Iterator<Map.Entry<Double, Double>> itr1 = partial_sum.entrySet().iterator();
            
            while (itr1.hasNext()) {
                Entry<Double, Double> entry1 = itr1.next();
                Set record_count_set = record_count.entrySet();
                Double s_id_1 = entry1.getKey();
                Double partial_sum_1 = (Double)entry1.getValue();
                Double record_count_1 = (Double)record_count.get(s_id_1);
                // theLogger.info("------------"+Double.toString(partial_sum_1));
                context.write(new DoubleWritable(s_id_1), new DoublePair(new DoubleWritable(partial_sum_1), new DoubleWritable(record_count_1)));
            }
        } // end of cleanup
    } // end of mapper class
    
    
    public static class ComputeAverageInMapCombReduce extends Reducer<DoubleWritable, DoublePair, DoubleWritable, DoubleWritable> {
        
        protected void reduce(DoubleWritable key, Iterable<DoublePair> values, Reducer<DoubleWritable, DoublePair, DoubleWritable, DoubleWritable>.Context context) throws IOException, InterruptedException {
            Double s_id = key.get();
            Double sum = new Double(0);
            Double cnt = new Double(0);
            
            for (DoublePair value:values) {
                sum = sum.doubleValue() + value.getFirst().get();
                cnt = cnt.doubleValue() + value.getSecond().get();
            }
            Double avg_m = (Double) (sum.doubleValue()/cnt.doubleValue());
            context.write(new DoubleWritable(s_id), new DoubleWritable(avg_m));
        }
    }
    
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        args = new GenericOptionsParser(conf, args).getRemainingArgs();
        String input = args[0];
        String output = args[1];
        
        Job job = new Job(conf, "ComputeAverageInMapComb");
        job.setJarByClass(ComputeAverageInMapCombMap.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(ComputeAverageInMapCombMap.class);
        job.setMapOutputKeyClass(DoubleWritable.class);
        job.setMapOutputValueClass(DoublePair.class);
        
        job.setReducerClass(ComputeAverageInMapCombReduce.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(DoubleWritable.class);
        job.setOutputValueClass(DoubleWritable.class);
        
        FileInputFormat.setInputPaths(job, new Path(input));
        Path outPath = new Path(output);
        FileOutputFormat.setOutputPath(job, outPath);
        outPath.getFileSystem(conf).delete(outPath, true);
        
        job.waitForCompletion(true);
        return (job.waitForCompletion(true) ? 0 : 1);
    }
    
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new ComputeAverageInMapComb(), args);
        System.exit(exitCode);
    }
}
