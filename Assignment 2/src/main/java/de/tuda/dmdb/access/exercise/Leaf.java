package de.tuda.dmdb.access.exercise;

import de.tuda.dmdb.access.AbstractIndexElement;
import de.tuda.dmdb.access.LeafBase;
import de.tuda.dmdb.access.RecordIdentifier;
import de.tuda.dmdb.access.UniqueBPlusTreeBase;
import de.tuda.dmdb.storage.AbstractRecord;
import de.tuda.dmdb.storage.types.AbstractSQLValue;
import de.tuda.dmdb.storage.types.exercise.SQLInteger;

/**
 * Index leaf
 * 
 * @author cbinnig
 */
public class Leaf<T extends AbstractSQLValue> extends LeafBase<T> {

	/**
	 * Leaf constructor
	 * 
	 * @param uniqueBPlusTree
	 *            TODO
	 */
	public Leaf(UniqueBPlusTreeBase<T> uniqueBPlusTree) {
		super(uniqueBPlusTree);
	}

	/**
	 * @param key
	 *            key of the record to find
	 * @return if found, returns the record, otherwise null
	 */
	@Override
	public AbstractRecord lookup(T key) {
		// TODO: implement this method

		AbstractRecord rid = uniqueBPlusTree.getLeafRecPrototype().clone();
		int pos = binarySearch(key);

		if (pos >= indexPage.getNumRecords())
			return null;

		// lookup index at position pos and read the key
		indexPage.read(pos, rid);
		AbstractSQLValue keyValue = rid.getValue(UniqueBPlusTreeBase.KEY_POS);

		// if the key at position pos equals the given key then the record exists and
		// gets returned
		if (key.compareTo(keyValue) == 0)
			return uniqueBPlusTree.getTable().lookup(((SQLInteger) rid.getValue(UniqueBPlusTree.PAGE_POS)).getValue(),
					((SQLInteger) rid.getValue(UniqueBPlusTree.SLOT_POS)).getValue());

		return null;
	}

	/**
	 * @param key
	 *            key of the record
	 * @param record
	 *            record to be inserted
	 * @return true if insertion was correct, false otherwise
	 */
	@Override
	public boolean insert(T key, AbstractRecord record) {
		// TODO: implement this method
		// search for key and return false if existing
		if (lookup(key) == null) {
			// return false if leaf is full to propagate split upwards
			if (isFull())
				return false;

			// else insert the record and insert a new RID into the index page
			RecordIdentifier rid = uniqueBPlusTree.getTable().insert(record);

			int indexToInsert = binarySearch(key);
			AbstractRecord ptr = uniqueBPlusTree.getLeafRecPrototype().clone();
			ptr.setValue(0, key);
			ptr.setValue(1, new SQLInteger(rid.getPageNumber()));
			ptr.setValue(2, new SQLInteger(rid.getSlotNumber()));

			indexPage.insert(indexToInsert, ptr, true);

			return true;
		} else
			return false;
	}

	@Override
	public AbstractIndexElement<T> createInstance() {
		return new Leaf<T>(this.uniqueBPlusTree);
	}
}