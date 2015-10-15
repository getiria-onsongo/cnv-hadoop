import java.math.BigDecimal;

/**
 * Compute moving average by using an array data structure.
 *
 * @author Getiria Onsongo 
 */

public class MovingAverageUsingArray {
    
    private double[] data;
    private double[] temp_data;
    private double[] ans;
    private int[] chrpos;
    private int[] chrpos_temp;
    private int[] temp_chrpos;
    private int windowEnd = 0;
    private int windowSize = 0;
    private int arrayLength = 0;
    private int ans_pos = 0;
    private int period;
    private int chr_pointer;
    private SimpleMovingAverageUsingArray sma;
    
    public MovingAverageUsingArray(int[] chrpos, double[] data, int windowSize) {
        if (data.length < 1) {
            throw new IllegalArgumentException("Input must be array of numbers > 0");
        }
        this.chrpos = chrpos;
        this.data = data;
        this.ans = new double[data.length];
        this.windowSize = windowSize;
    }
    
    public double[] getArrayMovingAverage() {
        arrayLength=data.length;
        int temp_length = windowSize * 4;
        temp_chrpos = new int[temp_length];
        temp_data = new double[temp_length];
        int i = 0;
        
        while(i < arrayLength) {
            if(i < temp_length){
                temp_chrpos[i] = chrpos[i];
                temp_data[i] = data[i];
            } else if (i == temp_length) {
                period = (windowSize + 1)/2;
                chrpos_temp = new int[period];
                chr_pointer = 0;
                sma = new SimpleMovingAverageUsingArray(windowSize);
                for(int cnt=0; cnt < temp_length; cnt++){
                    if(cnt < ((windowSize + 1)/2)){
                        chr_pointer = chr_pointer % period;
                        chrpos_temp[chr_pointer] = temp_chrpos[windowEnd];
                        sma.addNewNumber(temp_data[windowEnd]);
                        windowEnd++;
                        chr_pointer++;
                    }else{
                        chr_pointer = chr_pointer % period;
                        ans[ans_pos] = sma.getMovingAverage();
                        System.out.println("counter: " + ans_pos + "chrpos: " + chrpos_temp[chr_pointer] + "\t value: "+ans[ans_pos]);
                        chrpos_temp[chr_pointer] = temp_chrpos[windowEnd];
                        ans_pos++;
                        sma.addNewNumber(temp_data[windowEnd]);
                        windowEnd++;
                        chr_pointer++;
                    }
                }
                chr_pointer = chr_pointer % period;
                ans[ans_pos] = sma.getMovingAverage();
                System.out.println("counter: " + ans_pos + "chrpos: " + chrpos_temp[chr_pointer] + "\t value: "+ans[ans_pos]);
                chrpos_temp[chr_pointer] = chrpos[i];
                ans_pos++;
                windowEnd ++;
                chr_pointer ++;
                sma.addNewNumber(data[i]);
            }else{
                chr_pointer = chr_pointer % period;
                ans[ans_pos] = sma.getMovingAverage();
                System.out.println("counter: " + ans_pos + "chrpos: " + chrpos_temp[chr_pointer] + "\t value: "+ans[ans_pos]);
                chrpos_temp[chr_pointer] = chrpos[i];
                ans_pos++;
                chr_pointer++;
                windowEnd++;
                sma.addNewNumber(data[i]);
            }
            i++;
        }
    
        if(i < temp_length){
            // total amount to data seen was less than 4 x WindowSize
            // redefine window size and then smooth
            windowSize = (i/4);
            period = (windowSize + 1)/2;
            chrpos_temp = new int[period];
            chr_pointer = 0;
            sma = new SimpleMovingAverageUsingArray(windowSize);
            
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
                ans[ans_pos] = sma.getMovingAverage();
                System.out.println("counter: " + ans_pos + "chrpos: " + chrpos_temp[chr_pointer] + "\t value: "+ans[ans_pos]);
                ans_pos++;
                chr_pointer++;
            }
            
        }else{
            while(ans_pos < windowEnd){
                ans[ans_pos] = sma.getMovingAverage();
                chr_pointer = chr_pointer % period;
                System.out.println("counter: " + ans_pos + "chrpos: " + chrpos_temp[chr_pointer] + "\t value: "+ans[ans_pos]);
                sma.removeNumber();
                ans_pos++;
                chr_pointer++;
                
            }
        
        }
        System.out.println("windowSize: "+windowSize);
        return ans;
    }
}
