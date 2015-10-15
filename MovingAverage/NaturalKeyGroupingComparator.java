import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * 
 * NaturalKeyGroupingComparator
 * 
 * This class is used during Hadoop's shuffle phase to group 
 * composite key's by the first part (natural) of their key.
 * The natural key for coverage data is the "genesymbol".
 *
 * @author Getiria Onsongo
 *
 */
public class NaturalKeyGroupingComparator extends WritableComparator {

	protected NaturalKeyGroupingComparator() {
		super(CompositeKey.class, true);
	}

	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		CompositeKey key1 = (CompositeKey) w1;
		CompositeKey key2 = (CompositeKey) w2;
		return key1.getGeneSymbol().compareTo(key2.getGeneSymbol());
	}

}
