/**
 * Simple moving average by using an array data structure.
 *
 * @author Getiria Onsongo (modified code by Mahmoud Parsian).
 *
 */
public class SimpleMovingAverageUsingArray {
    
    private double sum = 0.0;
    private final int period;
    private double[] window = null;
    private int pointer = 0;
    private int size = 0;
    
    public SimpleMovingAverageUsingArray(int period) {
        if (period < 1) {
            throw new IllegalArgumentException("period must be > 0");
        }
        this.period = period;
        window = new double[period];
    }
    
    public void addNewNumber(double number) {
        sum = sum + number;
        if (size < period) {
            window[pointer] = number;
            pointer = pointer + 1;
            size = size + 1;
        }
        else {
            // size = period (size cannot be > period)
            pointer = pointer % period;
            sum = sum - window[pointer];
            window[pointer] = number;
            pointer = pointer + 1;
        }
    }
    
    public void removeNumber() {
        pointer = pointer % period;
        sum = sum - window[pointer];
        size = size - 1;
        pointer = pointer + 1;
    }
    
    public double getMovingAverage() {
        if (size == 0) {
            throw new IllegalArgumentException("average is undefined");
        }
        return sum / size;
    }
}
