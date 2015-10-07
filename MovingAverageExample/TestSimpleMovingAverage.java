import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.LogManager;

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

        // time series        0   1   2  3   4   5   6    7   8  9
        double[] testData = {10, 18, 20, 30, 24, 33, 27, 56, 21, 45};
        arrayLength=testData.length;
        windowSize = Integer.parseInt(args[0]);
        
        SimpleMovingAverageUsingArray sma = new SimpleMovingAverageUsingArray(windowSize);
    
        for (int i=0; i<arrayLength;i++){
            if(i < ((windowSize + 1)/2)){
                windowEnd = i + ((i + i)/2);
                while(itemsPosition <= windowEnd){
                    sma.addNewNumber(testData[itemsPosition]);
                    itemsPosition++;
                }
                windowEnd = itemsPosition;
            }else{
                if(windowEnd >= arrayLength){
                    sma.removeNumber();
                }else{
                    sma.addNewNumber(testData[itemsPosition]);
                    windowEnd = windowEnd + 1;
                    itemsPosition = itemsPosition + 1;
                }
            }
            System.out.println("i= " + i + " original value= " + testData[i] + " SMA = " + sma.getMovingAverage());
        }
    }
}
