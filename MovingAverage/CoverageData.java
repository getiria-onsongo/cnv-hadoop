import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

/**
 * 
 * CoverageData represents a pair of
 *  (chromosome_position, coverage-value).
 *  
 * @author Getiria Onsongo
 *
 */
public class CoverageData
   implements Writable, Comparable<CoverageData> {

	private int chrpos;
	private double coverage;
	
	public static CoverageData copy(CoverageData covd) {
		return new CoverageData(covd.chrpos, covd.coverage);
	}
	
	public CoverageData(int chrpos, double coverage) {
		set(chrpos, coverage);
	}
	
	public CoverageData() {
	}
	
	public void set(int chrpos, double coverage) {
		this.chrpos = chrpos;
		this.coverage = coverage;
	}	
	
	public int getChrPos() {
		return this.chrpos;
	}
	
	public double getCoverage() {
		return this.coverage;
	}
	
	/**
	 * Deserializes the point from the underlying data.
	 * @param in a DataInput object to read the point from.
	 */
	public void readFields(DataInput in) throws IOException {
		this.chrpos  = in.readInt();
		this.coverage  = in.readDouble();
	}

	/**
	 * Convert a binary data into CoverageData
	 * 
	 * @param in A DataInput object to read from.
	 * @return A CoverageData object
	 * @throws IOException
	 */
	public static CoverageData read(DataInput in) throws IOException {
		CoverageData covData = new CoverageData();
		covData.readFields(in);
		return covData;
	}

   /**
    * Creates a clone of this object
    */
    public CoverageData clone() {
       return new CoverageData(chrpos, coverage);
    }

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(this.chrpos );
		out.writeDouble(this.coverage );

	}

	/**
	 * Used in sorting the data in the reducer
	 */
	@Override
	public int compareTo(CoverageData data) {
		if (this.chrpos  < data.chrpos ) {
			return -1;
		} 
		else if (this.chrpos  > data.chrpos ) {
			return 1;
		}
		else {
		   return 0;
		}
	}
	
	public String toString() {
       return "("+chrpos+","+coverage+")";
    }
}
