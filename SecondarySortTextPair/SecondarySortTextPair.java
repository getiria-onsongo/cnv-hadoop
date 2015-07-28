// cc SecondarySortTextPair Application to find the maximum temperature by sorting temperatures in the key

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
import org.apache.hadoop.util.*;



// vv SecondarySortTextPair
public class SecondarySortTextPair {
    
  static class CoverageMapper extends Mapper<LongWritable, Text, TextPair, Text> {
      private TextPair chrPos = new TextPair();

      @Override
      public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
          String line = value.toString();
          String delimiter = "\\t";
          String[] strArray = line.split(delimiter);

          chrPos.set(new Text(strArray[0]),new Text(strArray[1]));
          context.write(chrPos,new Text(strArray[2]));
      }
    
  }
 
  static class CoverageReducer extends Reducer<TextPair, Text, TextPair, Text> {
      
        @Override
        public void reduce(TextPair key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                context.write(key,value);
            }
        }
        
    }
  public static class FirstPartitioner extends Partitioner<TextPair, Text> {

    @Override
    public int getPartition(TextPair key, Text value, int numPartitions) {
      // multiply by 127 to perform some mixing
      // NOTE: The partitioner is only using the first Object in the key (chr in this case)
      // which ensures coverages from the same chromosome go to the same partition. For our
      // purposes, it would be better to use GENE instead og CHROMOSOME because chromosome is
      // not small enough a subset.
        String keyString = key.getFirst().toString();
        int hash = keyString.hashCode();
        return Math.abs((hash * 127) % numPartitions);
    }
  }
  
  public static class KeyComparator extends WritableComparator {
    protected KeyComparator() {
      super(TextPair.class, true);
    }

    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
      TextPair ip1 = (TextPair) w1;
      TextPair ip2 = (TextPair) w2;
      int cmp = ip1.getFirst().compareTo(ip2.getFirst());
      if (cmp != 0) {
        return cmp;
      }
      return ip1.getSecond().compareTo(ip2.getSecond());
    }
  }
  
  public static class GroupComparator extends WritableComparator {
    protected GroupComparator() {
      super(TextPair.class, true);
    }
    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
      TextPair ip1 = (TextPair) w1;
      TextPair ip2 = (TextPair) w2;
      return ip1.getFirst().compareTo(ip2.getFirst());
    }
  }
  
  public static void main(String[] args) throws Exception {
      
      if (args.length != 2) {
          System.err.println("Usage: SecondarySortTextPair <input file> <output path> ");
          System.exit(-1);
      }
      
      Job job = new Job();
      job.setJobName("SecondarySortTextPair");
      job.setJarByClass(SecondarySortTextPair.class);
      
      FileInputFormat.addInputPath(job, new Path(args[0]));
      FileOutputFormat.setOutputPath(job, new Path(args[1]));
      
      job.setMapperClass(CoverageMapper.class);
      job.setPartitionerClass(FirstPartitioner.class);
      job.setSortComparatorClass(KeyComparator.class);
      job.setGroupingComparatorClass(GroupComparator.class);
      job.setReducerClass(CoverageReducer.class);
      
      job.setMapOutputKeyClass(TextPair.class);
      job.setMapOutputValueClass(Text.class);
            
      System.exit(job.waitForCompletion(true) ? 0 : 1);
      
      
  }
}
// ^^ SecondarySortTextPair
