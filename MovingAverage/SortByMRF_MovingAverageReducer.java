import java.util.Iterator;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.JobConf;

/**
 * 
 * SortByMRF_MovingAverageReducer implements the reduce() function.
 * 
 * Data arrive sorted to reducer (reducers values are sorted by 
 * Mapreduce framework  -- NOTE: values are not sorted in memory).
 
  This class does most of the heavy lifting in noise reduction (smoothing)
  of coverage data. This is an implementation of a modified moving average
  algorithm. In moving average, last X number of points are used to
  compute moving average. In noise reduction/smoothing, a value is updated
  to be the average of X points to the left and X points to the right 
  (flanking) the data point. This makes implementation in Map/Reduce slightly 
  trickier. While in moving average you only need points seen so far to compute
  moving average, in noise reduction you need to consider X points
  that will come in future in addition to the X points seen so far where X is 
  (windowSize/2). In this implementation, two arrays are created that 
  temporarily store data points and only starts computing coverage once 
  (windowSize * 4) points have been seen. If fewer than (windowSize * 4) 
  points are seen, the window size is re-defined to be 1/4 the
  total number of points seen.
 
 *
 * @author Getiria Onsongo
 *
 */ 
public class SortByMRF_MovingAverageReducer extends MapReduceBase 
   implements Reducer<CompositeKey, CoverageData, Text, Text> {
    int windowSize = 5; // default window size
	/**
	 *  will be run only once to get user defined windowSize from Hadoop's 
     configuration
	 */
	@Override
	public void configure(JobConf jobconf) {
        this.windowSize = jobconf.getInt("moving.average.window.size", 5);
	}
	public void reduce(CompositeKey key, 
	                   Iterator<CoverageData> values,
			           OutputCollector<Text, Text> output, 
			           Reporter reporter)
		throws IOException {
		// note that values are sorted.
        // apply smoothing average algorithm to sorted coverage data

        int windowEnd = 0; // Numbe of points seen
        int chr_pointer = 0;
        windowSize = this.windowSize;
        int period = (windowSize + 1)/2;
        int[] chrpos_temp = new int[period]; // Need to store chrpos so we
        // can retrieve it when enough points to compute sliding average
        // have been seen
    
        Text outputKey = new Text();
        Text outputValue = new Text();
        
        int temp_length = windowSize * 4;
        int[] temp_chrpos = new int[temp_length];
        double[] temp_data = new double[temp_length];
        int chrpos;
        double coverage;
        int out_chrpos;
        double out_movingAverage;
        int i = 0;
        int ans_pos = 0; // Number of points for which we have computed
            // sliding average
        
        MovingAverage sma = new MovingAverage(windowSize);
        
        // While loop that gets values coming to the reducer
        while (values.hasNext()) {
            CoverageData data = values.next();
            chrpos = data.getChrPos();
            coverage = data.getCoverage();
            
            if(i < temp_length){
                // Temporarily store data to make sure (windowSize x 4)
                // data points are seen before you start computing moving
                // average.
                temp_chrpos[i] = chrpos;
                temp_data[i] = coverage;
            } else if (i == temp_length) {
                // We have enough points to start computing sliding window
                // average
                period = (windowSize + 1)/2;
                chrpos_temp = new int[period];
                chr_pointer = 0;
                sma = new MovingAverage(windowSize);
                for(int cnt=0; cnt < temp_length; cnt++){
                    if(cnt < ((windowSize + 1)/2)){
                        // Accumilate (windowSize/2) points before you start
                        // computing average. NOTE: Point for which we are
                        // computing average will lag behind datapoints seen
                        // by (windowSize/2)
                        chr_pointer = chr_pointer % period;
                        chrpos_temp[chr_pointer] = temp_chrpos[windowEnd];
                        sma.addNewNumber(temp_data[windowEnd]);
                        windowEnd++;
                        chr_pointer++;
                    }else{
                        // Compute sliding window average for points in temp
                        // arrays
                        chr_pointer = chr_pointer % period;
                        out_movingAverage = sma.getMovingAverage();
                        out_chrpos = chrpos_temp[chr_pointer];
                        outputValue.set(out_chrpos + "\t" + out_movingAverage);
                        outputKey.set(key.getGeneSymbol());
                        output.collect(outputKey, outputValue);
                        chrpos_temp[chr_pointer] = temp_chrpos[windowEnd];
                        sma.addNewNumber(temp_data[windowEnd]);
                        windowEnd++;
                        chr_pointer++;
                        ans_pos++;
                    }
                }
                // We have computed sliding window average for all points in
                // temp array now move to the data point just seen. NOTE: sliding
                // average lags behind points seen by (windowSize/2). That is why
                // we needed an array to store chr positions. This also explains
                // why period = (windowSize + 1)/2. The modulus function helps us
                // retrieve chr position (windowSize + 1)/2 behind the point we
                // just saw
                chr_pointer = chr_pointer % period;
                out_movingAverage = sma.getMovingAverage();
                out_chrpos = chrpos_temp[chr_pointer];
                outputValue.set(out_chrpos + "\t" + out_movingAverage);
                outputKey.set(key.getGeneSymbol());
                output.collect(outputKey, outputValue);
                chrpos_temp[chr_pointer] = chrpos;
                windowEnd ++;
                chr_pointer ++;
                ans_pos++;
                sma.addNewNumber(coverage);
            }else{
                // This section computes sliding average until we have no more
                // points coming to the reducer
                chr_pointer = chr_pointer % period;
                out_movingAverage = sma.getMovingAverage();
                out_chrpos = chrpos_temp[chr_pointer];
                outputValue.set(out_chrpos + "\t" + out_movingAverage);
                outputKey.set(key.getGeneSymbol());
                output.collect(outputKey, outputValue);
                chrpos_temp[chr_pointer] = chrpos;
                chr_pointer++;
                windowEnd++;
                ans_pos++;
                sma.addNewNumber(coverage);
            }
            i++;
        } // while
           
            if(i < temp_length){
                // If we got to this block, it means the total amount to data
                // seen was less than 4 x WindowSize re-define window size
                // and then smooth
                windowSize = (i/4);
                period = (windowSize + 1)/2;
                chrpos_temp = new int[period];
                chr_pointer = 0;
                sma = new MovingAverage(windowSize);
                for(int cnt=0; cnt<i; cnt++){
                    if(cnt < ((windowSize + 1)/2)){
                        chr_pointer = chr_pointer % period;
                        chrpos_temp[chr_pointer] = temp_chrpos[windowEnd];
                        sma.addNewNumber(temp_data[windowEnd]);
                        windowEnd++;
                    }else{
                        if(windowEnd >= i){
                            sma.removeNumber();
                        }else{
                            chr_pointer = chr_pointer % period;
                            chrpos_temp[chr_pointer] = temp_chrpos[windowEnd];
                            sma.addNewNumber(temp_data[windowEnd]);
                            windowEnd++;
                        }
                    }
                    out_movingAverage = sma.getMovingAverage();
                    out_chrpos = chrpos_temp[chr_pointer];
                    outputValue.set(out_chrpos + "\t" + out_movingAverage);
                    outputKey.set(key.getGeneSymbol());
                    output.collect(outputKey, outputValue);
                    chr_pointer++;
                    ans_pos++;
                }
            }else{
                // If we get to this block, it means the reducer saw more points
                // than (4 x WindowSize) which means our computation for sliding
                // average lags behind points seen by (windowSize/2). The variable
                // windowEnd keeps track of number of data points seen while the
                // variable ans_pos keeps track of number of points for which we've
                // calculated sliding average. The while loop below computes sliding
                // average for the remaining points.
                while(ans_pos < windowEnd){
                    out_movingAverage = sma.getMovingAverage();
                    chr_pointer = chr_pointer % period;
                    out_chrpos = chrpos_temp[chr_pointer];
                    outputValue.set(out_chrpos + "\t" + out_movingAverage);
                    outputKey.set(key.getGeneSymbol());
                    output.collect(outputKey, outputValue);
                    sma.removeNumber();
                    chr_pointer++;
                    ans_pos++;
                }
                
            } // else
        } // reduce
   } // SortByMRF_MovingAverageReducer
