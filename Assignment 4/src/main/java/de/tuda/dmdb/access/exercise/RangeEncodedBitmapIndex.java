package de.tuda.dmdb.access.exercise;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeMap;

import de.tuda.dmdb.access.AbstractBitmapIndex;
import de.tuda.dmdb.access.AbstractTable;
import de.tuda.dmdb.storage.AbstractRecord;
import de.tuda.dmdb.storage.types.AbstractSQLValue;

/**
 * Bitmap index that uses the range encoded approach (still one bitmap for each
 * distinct value)
 * 
 * @author lthostrup
 *
 ** @param <T>
 *            Type of the key index by the index. While all abstractSQLValues
 *            subclasses can be used, the implementation currently only support
 *            for SQLInteger type is guaranteed.
 */
public class RangeEncodedBitmapIndex<T extends AbstractSQLValue> extends AbstractBitmapIndex<T> {

	/*
	 * Constructor of RangeEncodedBitmapIndex
	 * 
	 * @param table Table for which the bitmap index will be build
	 * 
	 * @param keyColumnNumber: index of the column within the passed table that
	 * should be indexed
	 */
	public RangeEncodedBitmapIndex(AbstractTable table, int keyColumnNumber) {
		super(table, keyColumnNumber);
		this.bitMaps = new TreeMap<T, BitSet>(); // Use TreeMap to get an ordered map impl.
		this.bitmapSize = this.getTable().getRecordCount();
		this.bulkLoadIndex();
	}

	@SuppressWarnings("unchecked")
	@Override
	protected void bulkLoadIndex() {
		// TODO implement this method
		Iterator<AbstractRecord> iter = table.iterator();

		int pos = 0;
		// Iterate through table and fill the bitmaps
		while (iter.hasNext()) {
			T value = (T) iter.next().getValue(keyColumnNumber);

			// if key is already contained, just set a bit in the bitset
			if (bitMaps.containsKey(value)) {
				bitMaps.get(value).set(pos);
			}
			// if key is not yet contained, add it and set a bit in the bitset
			else {
				BitSet set = new BitSet(bitmapSize);
				set.set(pos);
				bitMaps.put(value, set);
			}
			pos++;
		}

		// after builduing the bitmaps with the naive approach, do all the or
		// calculations. Calculation: Calculate logical OR with the bitset of every key and the bitsets of all of its
		// higher predecessors.
		for (T key : ((TreeMap<T, BitSet>) bitMaps).descendingKeySet()) {
			if (((TreeMap<T, BitSet>) bitMaps).higherKey(key) != null) {
				BitSet set = bitMaps.get(key);
				set.or(((TreeMap<T, BitSet>) bitMaps).higherEntry(key).getValue());
				bitMaps.put(key, set);
			}
		}
	}

	@Override
	public List<AbstractRecord> rangeLookup(T startKey, T endKey) {
		ArrayList<AbstractRecord> records = new ArrayList<>();

		//Calculation: bitset of startkey AND NOT bitset of next higher key than endKey. If no key is higher than end key, skip the AND NOT calculation
		Entry<T, BitSet> higherEntry = ((TreeMap<T, BitSet>) bitMaps).higherEntry(endKey);
		BitSet lower = bitMaps.get(startKey);

		if (higherEntry != null)
			lower.andNot(higherEntry.getValue());

		for (int i = lower.nextSetBit(0); i != -1; i = lower.nextSetBit(i + 1)) {
			records.add(table.getRecordFromRowId(i));
		}

		return records;
	}

}
