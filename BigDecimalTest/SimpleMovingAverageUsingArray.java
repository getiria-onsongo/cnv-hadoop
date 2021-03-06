import org.nevec.rjm.*;
import java.math.BigDecimal;

/**
 * Simple moving average by using an array data structure.
 *
 * @author Getiria Onsongo (modified code by Mahmoud Parsian).
 *
 * NOTE: We are using the package org.nevec.rjm to enable us compute in log space using BigDecimal
 */
public class SimpleMovingAverageUsingArray {
    
    private BigDecimal sum = new BigDecimal(0.0);
    private final int period;
    private BigDecimal[] window = null;
    private int pointer = 0;
    private int size = 0;
    
    public SimpleMovingAverageUsingArray(int period) {
        if (period < 1) {
            throw new IllegalArgumentException("period must be > 0");
        }
        this.period = period;
        window = new BigDecimal[period];
    }
    
    public void addNewNumber(double inputNumber) {
        BigDecimal logOfInputNumber = new BigDecimal(Math.log(inputNumber));
        BigDecimal logOf2 = new BigDecimal(Math.log(2));
        BigDecimal number = BigDecimalMath.divideRound(logOfInputNumber,logOf2);
        // System.out.println("input: " + inputNumber + "\t log2(input): " + number);
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
        BigDecimal twoDouble = new BigDecimal;
        //System.out.println("sum: " + sum + "\t size: " + size + "\t avgInLog2: " + avgInLog2);
        double ans = Math.pow(twoDouble,avgInLog2.doubleValue());
        return ans;
    }
}
