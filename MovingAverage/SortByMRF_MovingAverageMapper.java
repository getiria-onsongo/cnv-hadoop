import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.commons.lang.StringUtils;


/**
 * 
 * SortByMRF_MovingAverageMapper implements the map() function.
 *  
 * @author Getiria Onsongo
 *
 */ 
public class SortByMRF_MovingAverageMapper extends MapReduceBase 
   implements Mapper<LongWritable, Text, CompositeKey, CoverageData> {
 
   // reuse Hadoop's Writable objects
   private final CompositeKey reducerKey = new CompositeKey();
   private final CoverageData reducerValue = new CoverageData();
 
	@Override
	public void map(LongWritable inkey, Text value,
			OutputCollector<CompositeKey, CoverageData> output,
			Reporter reporter) throws IOException {
	   String record = 	value.toString();
	   if ( (record == null) || (record.length() ==0) ) {
	      return;
	   }			   
       String[] tokens = StringUtils.split(record, "\t");
       if (tokens.length == 3) {
          // tokens[0] = genesymbol
          // tokens[1] = chrpos
          // tokens[2] = coverage

          reducerKey.set(tokens[0], Integer.parseInt(tokens[1]));
          reducerValue.set(Integer.parseInt(tokens[1]), Double.parseDouble(tokens[2]));
          // emit key-value pair
          output.collect(reducerKey, reducerValue);
       }
       else {
          // log as error, not enough tokens
       }
   }
}
