import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.LogManager;

import java.math.BigDecimal;

/** 
 * Basic testing of Simple moving average.
 *
 * @author Mahmoud Parsian
 *
 */
public class TestSimpleMovingAverage {
    private static int windowEnd = 0;
    private static int windowSize = 0;
    private static int arrayLength =0;
    private static int itemsPosition = 0;
    
    public static void main(String[] args) {
        int[] chrpos = {33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65};
        // time series        0   1     2     3   4    5     6    7    8     9   10  11   12
        double[] testData = {
            0.5,1.00,2.0,0.70,0.90,1.25,0.8,1.00,1.25,0.8,1.00,
            0.8,1.25,0.5,0.80,2.00,1.00,0.5,1.25,2.00,0.8,1.25,
            0.8,0.80,1.25,1.25,1.25,0.80,1.0,0.80,1.25,1.7,2.00};
        windowSize = Integer.parseInt(args[0]);
        
        MovingAverageUsingArray initDataStruct = new MovingAverageUsingArray(chrpos,testData,windowSize);
        double[] smoothedData = initDataStruct.getArrayMovingAverage();
        System.out.println("");
        for (int i=0; i<smoothedData.length;i++){
            System.out.println("counter: " + i + "\t input: "+ testData[i] + "\t avg: " + smoothedData[i]);
        }
    
    }
}
