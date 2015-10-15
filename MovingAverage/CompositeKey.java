import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * 
 * CompositeKey: represents a pair of 
 * (String genesymbol, int chrpos).
 * 
 * 
 * We do a primary grouping pass on the gene symbol field to get all of the data of
 * one type together, and then our "secondary sort" during the shuffle phase
 * uses the chrpos int member to sort the coverage data points so that they
 * arrive at the reducer partitioned and in sorted order.
 * 
 * 
 * @author Getiria Onsongo
 *
 */
public class CompositeKey implements WritableComparable<CompositeKey> {
    // natural key is (gene symbol)
    // composite key is a pair (genesymbol, chrpos)
	private String genesymbol;
	private int chrpos;

	public CompositeKey(String genesymbol, int chrpos) {
		set(genesymbol, chrpos);
	}
	
	public CompositeKey() {
	}

	public void set(String genesymbol, int chrpos) {
		this.genesymbol = genesymbol;
		this.chrpos = chrpos;
	}

	public String getGeneSymbol() {
		return this.genesymbol;
	}

	public int getChrPos() {
		return this.chrpos;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.genesymbol = in.readUTF();
		this.chrpos = in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(this.genesymbol);
		out.writeInt(this.chrpos);
	}

	@Override
	public int compareTo(CompositeKey other) {
		if (this.genesymbol.compareTo(other.genesymbol) != 0) {
			return this.genesymbol.compareTo(other.genesymbol);
		} 
		else if (this.chrpos != other.chrpos) {
			return chrpos < other.chrpos ? -1 : 1;
		} 
		else {
			return 0;
		}

	}

	public static class CompositeKeyComparator extends WritableComparator {
		public CompositeKeyComparator() {
			super(CompositeKey.class);
		}

		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			return compareBytes(b1, s1, l1, b2, s2, l2);
		}
	}

	static { // register this comparator
		WritableComparator.define(CompositeKey.class,
				new CompositeKeyComparator());
	}

}
