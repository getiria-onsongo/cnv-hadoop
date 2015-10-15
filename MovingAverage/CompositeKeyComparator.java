import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * CompositeKeyComparator
 * 
 * The purpose of this class is to enable comparison of two CompositeKey(s).
 * 
 * @author Getiria Onsongo
 *
 */
public class CompositeKeyComparator extends WritableComparator {

	protected CompositeKeyComparator() {
		super(CompositeKey.class, true);
	}

	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		CompositeKey key1 = (CompositeKey) w1;
		CompositeKey key2 = (CompositeKey) w2;

		int comparison = key1.getGeneSymbol().compareTo(key2.getGeneSymbol());
		if (comparison == 0) {
			 // names are equal here
		     if (key1.getChrPos() == key2.getChrPos()) {
		     	return 0;
		     }
		     else if (key1.getChrPos() < key2.getChrPos()) {
		     	return -1;
		     }
		     else {
		     	return 1;
		     }
		}
		else {
			return comparison;
		}
	}
}
