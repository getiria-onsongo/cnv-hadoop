// cc SecondarySortTextIntPairTextIntPair Application to find the maximum temperature by sorting temperatures in the key

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



// vv SecondarySortTextIntPair
public class SecondarySortTextIntPair {
    
  static class CoverageMapper extends Mapper<LongWritable, Text, TextIntPair, IntWritable> {
      private TextIntPair chrPos = new TextIntPair();

      @Override
      public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
          String line = value.toString();
          String delimiter = "\\t";
          String[] strArray = line.split(delimiter);

          chrPos.set(new Text(strArray[0]),new IntWritable(Integer.parseInt(strArray[1])));
          context.write(chrPos,new IntWritable(Integer.parseInt(strArray[2])));
      }
    
  }
 
  static class CoverageReducer extends Reducer<TextIntPair, IntWritable, TextIntPair, IntWritable> {
      
        @Override
        public void reduce(TextIntPair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            for (IntWritable value : values) {
                context.write(key,value);
            }
        }
        
    }
  public static class FirstPartitioner extends Partitioner<TextIntPair, IntWritable> {

    @Override
    public int getPartition(TextIntPair key, IntWritable value, int numPartitions) {
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
      super(TextIntPair.class, true);
    }

    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
      TextIntPair ip1 = (TextIntPair) w1;
      TextIntPair ip2 = (TextIntPair) w2;
      int cmp = ip1.getFirst().compareTo(ip2.getFirst());
      if (cmp != 0) {
        return cmp;
      }
      return ip1.getSecond().compareTo(ip2.getSecond());
    }
  }
  
  public static class GroupComparator extends WritableComparator {
    protected GroupComparator() {
      super(TextIntPair.class, true);
    }
    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
      TextIntPair ip1 = (TextIntPair) w1;
      TextIntPair ip2 = (TextIntPair) w2;
      return ip1.getFirst().compareTo(ip2.getFirst());
    }
  }
  
  public static void main(String[] args) throws Exception {
      
      if (args.length != 2) {
          System.err.println("Usage: SecondarySortTextIntPair <input file> <output path> ");
          System.exit(-1);
      }
      
      Job job = new Job();
      job.setJobName("SecondarySortTextIntPair");
      job.setJarByClass(SecondarySortTextIntPair.class);
      
      FileInputFormat.addInputPath(job, new Path(args[0]));
      FileOutputFormat.setOutputPath(job, new Path(args[1]));

      job.setOutputKeyClass(TextIntPair.class);
      job.setOutputValueClass(IntWritable.class);
      
      job.setMapperClass(CoverageMapper.class);
      
      // Partitioner ensures records with the same "first" entry end up in the same partition
      job.setPartitionerClass(FirstPartitioner.class);
      // SortComparator sorts using "second" entry
      job.setSortComparatorClass(KeyComparator.class);
      // GroupingComparator ensure we group by natural key (first) and not composite key (first,second)
      job.setGroupingComparatorClass(GroupComparator.class);
      
      job.setReducerClass(CoverageReducer.class);
      
      System.exit(job.waitForCompletion(true) ? 0 : 1);
      
      
  }
}
// ^^ SecondarySortTextIntPair
