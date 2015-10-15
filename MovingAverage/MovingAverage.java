import java.math.BigDecimal;

/**
 * 
 * This class, MovingAverage, implements the basic functionality of
 * "moving average" algorithm. 
 * 
 * This class is used during Hadoop's shuffle phase to group 
 * composite key's by the first part (natural) of their key.
 * The natural key for coverage data is the "genesymbol".
 *
 * @author Getiria Onsongo
 *
 * NOTE: We are working in log2 space because values are symmetrical around 1.0. i.e average of 0.5 and 2.0 if 1
 * in log2 space but > 1 when using real numbers space. Also note, it is only advisable to work in log 2 space
 * when you are dealing with ratios closer to 2 e.g. between 0.5 and 2.0.  If ratios are significantly greater
 * than 2, you will lose precision. Try computing average in log2 space for numers such as 10, 18 and 20 to see
 * the effect
 */
public class MovingAverage {
    
    private BigDecimal sum = new BigDecimal(0.0);
    private final int period;
    private BigDecimal[] window = null;
    private int pointer = 0;
    private int size = 0;
    
    public MovingAverage(int period) {
        if (period < 1) {
            throw new IllegalArgumentException("period must be > 0");
        }
        this.period = period;
        window = new BigDecimal[period];
    }
    
    public void addNewNumber(double inputNumber) {
        BigDecimal logOfInputNumber = new BigDecimal(Math.log(inputNumber));
        BigDecimal logOf2 = new BigDecimal(Math.log(2));
        BigDecimal number = logOfInputNumber.divide(logOf2,46,BigDecimal.ROUND_HALF_EVEN);
        
        sum = sum.add(number);
        if (size < period) {
            window[pointer] = number;
            pointer = pointer + 1;
            size = size + 1;
        }
        else {
            // size = period (size cannot be > period)
            pointer = pointer % period;
            sum = sum.subtract(window[pointer]);
            window[pointer] = number;
            pointer = pointer + 1;
        }
    }
    
    public void removeNumber() {
        pointer = pointer % period;
        sum = sum.subtract(window[pointer]);
        size = size - 1;
        pointer = pointer + 1;
    }
    
    public double getMovingAverage() {
        if (size == 0) {
            throw new IllegalArgumentException("average is undefined");
        }
        
        BigDecimal avgInLog2 = sum.divide(new BigDecimal(size),46,BigDecimal.ROUND_HALF_EVEN);
        double twoDouble = 2.0;
        //System.out.println("sum: " + sum + "\t size: " + size + "\t avgInLog2: " + avgInLog2);
        double ans = Math.pow(twoDouble,avgInLog2.doubleValue());
        return ans;
    }
}
