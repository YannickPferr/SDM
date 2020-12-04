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
 * Bitmap that uses the approximate bitmap index (compressed) approach
 * 
 * @author melhindi
 *
 * @param <T>
 *            Type of the key index by the index. While all abstractSQLValues
 *            subclasses can be used, the implementation currently only support
 *            for SQLInteger type is guaranteed.
 */
public class ApproximateBitmapIndex<T extends AbstractSQLValue> extends AbstractBitmapIndex<T> {

	/*
	 * Constructor of ApproximateBitmapIndex This implementation uses modulo as hash
	 * function and only supports SQLInteger as data type
	 * 
	 * @param table Table for which the bitmap index will be build
	 * 
	 * @param keyColumnNumbner: index of the column within the passed table that
	 * should be indexed
	 * 
	 * @param bitmapSize Size of for each bitmap, i.e., use (% bitmapSize) as
	 * hashfunction
	 */
	public ApproximateBitmapIndex(AbstractTable table, int keyColumnNumber, int bitmapSize) {
		super(table, keyColumnNumber);
		this.bitMaps = new HashMap<T, BitSet>();
		this.bitmapSize = bitmapSize;
		this.bulkLoadIndex();
	}

	@SuppressWarnings("unchecked")
	@Override
	protected void bulkLoadIndex() {
		Iterator<AbstractRecord> iter = table.iterator();

		int pos = 0;
		// Iterate through table and fill the bitmaps
		while (iter.hasNext()) {
			T value = (T) iter.next().getValue(keyColumnNumber);

			// if key is already contained, just set a bit in the bitset but use the
			// hashfunction for calculating the position
			if (bitMaps.containsKey(value)) {
				bitMaps.get(value).set(pos % bitmapSize);
			}
			// if key is not yet contained, add it and set a bit in the bitset but use the
			// hashfunction for calculating the position
			else {
				BitSet set = new BitSet(bitmapSize);
				set.set(pos % bitmapSize);
				bitMaps.put(value, set);
			}
			pos++;
		}
	}

	@Override
	public List<AbstractRecord> rangeLookup(T startKey, T endKey) {
		ArrayList<AbstractRecord> records = new ArrayList<>();

		// iterate through the bitmaps and check if this key is between the range, if
		// yes check the values of the other qualifying rows and then lookup the rids
		// for all rows which value equals the key and add it to the record list
		for (T key : bitMaps.keySet()) {
			if (key.compareTo(startKey) >= 0 && key.compareTo(endKey) <= 0) {

				for (int i = bitMaps.get(key).nextSetBit(0); i != -1; i = bitMaps.get(key).nextSetBit(i + 1)) {
					for (int j = 0; j < table.getRecordCount() / bitmapSize; j++) {
						AbstractRecord record = table.getRecordFromRowId(i + (j * bitmapSize));

						if (record.getValue(keyColumnNumber).compareTo(key) == 0)
							records.add(record);
					}
				}
			}
		}

		return records;
	}
}
