package de.tuda.dmdb.access.exercise;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import de.tuda.dmdb.access.AbstractBitmapIndex;
import de.tuda.dmdb.access.AbstractTable;
import de.tuda.dmdb.storage.AbstractRecord;
import de.tuda.dmdb.storage.types.AbstractSQLValue;

/**
 * Bitmap index that uses the vanilla/naive bitmap approach (one bitmap for each
 * distinct value)
 * 
 * @author melhindi
 *
 ** @param <T>
 *            Type of the key index by the index. While all abstractSQLValues
 *            subclasses can be used, the implementation currently only support
 *            for SQLInteger type is guaranteed.
 */
public class NaiveBitmapIndex<T extends AbstractSQLValue> extends AbstractBitmapIndex<T> {

	/*
	 * Constructor of NaiveBitmapIndex
	 * 
	 * @param table Table for which the bitmap index will be build
	 * 
	 * @param keyColumnNumber: index of the column within the passed table that
	 * should be indexed
	 */
	public NaiveBitmapIndex(AbstractTable table, int keyColumnNumber) {
		super(table, keyColumnNumber);
		this.bitMaps = new HashMap<T, BitSet>();
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
	}

	@Override
	public List<AbstractRecord> rangeLookup(T startKey, T endKey) {
		// TODO implement this method
		ArrayList<AbstractRecord> records = new ArrayList<>();

		// iterate through the bitmaps and check if this key is between the range, if
		// yes lookup the rids and add it to the record list
		for (T key : bitMaps.keySet()) {
			if (key.compareTo(startKey) >= 0 && key.compareTo(endKey) <= 0) {

				for (int i = bitMaps.get(key).nextSetBit(0); i != -1; i = bitMaps.get(key).nextSetBit(i + 1)) {
					records.add(table.getRecordFromRowId(i));
				}
			}
		}

		return records;
	}

}
