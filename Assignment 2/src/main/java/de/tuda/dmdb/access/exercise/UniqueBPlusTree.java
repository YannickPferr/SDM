package de.tuda.dmdb.access.exercise;

import de.tuda.dmdb.access.AbstractIndexElement;
import de.tuda.dmdb.access.AbstractTable;
import de.tuda.dmdb.access.UniqueBPlusTreeBase;
import de.tuda.dmdb.storage.AbstractRecord;
import de.tuda.dmdb.storage.types.AbstractSQLValue;
import de.tuda.dmdb.storage.types.exercise.SQLInteger;

/**
 * Unique B+-Tree implementation
 * 
 * @author cbinnig
 *
 * @param <T>
 */
public class UniqueBPlusTree<T extends AbstractSQLValue> extends UniqueBPlusTreeBase<T> {

	/**
	 * Constructor of B+-Tree with user-defined fil-grade
	 * 
	 * @param table
	 *            Table to be indexed
	 * @param keyColumnNumber
	 *            Number of unique column which should be indexed
	 * @param fillGrade
	 *            fill grade of index
	 */
	public UniqueBPlusTree(AbstractTable table, int keyColumnNumber, int fillGrade) {
		super(table, keyColumnNumber, fillGrade);
	}

	/**
	 * Constructor for B+-tree with default fill grade
	 * 
	 * @param table
	 *            table to be indexed
	 * @param keyNumber
	 *            Number of unique column which should be indexed
	 */
	public UniqueBPlusTree(AbstractTable table, int keyColumnNumber) {
		this(table, keyColumnNumber, DEFAULT_FILL_GRADE);
	}

	/**
	 * @param record
	 *            record to be inserted
	 * @return true if insertion was correct, false otherwise
	 */
	@SuppressWarnings({ "unchecked" })
	@Override
	public boolean insert(AbstractRecord record) {
		// insert record
		// T key = (T) record.getValue(this.keyColumnNumber);
		// TODO: implement this method
		// recursively call insert on root node/leaf
		T key = (T) record.getValue(this.keyColumnNumber);
		boolean inserted = root.insert(key, record);

		// if insertion did work on child return true
		if (inserted)
			return inserted;
		else {
			AbstractIndexElement<T> newRoot = new Node<>(this);

			// split child
			AbstractIndexElement<T> leftTree = root.createInstance();
			AbstractIndexElement<T> rightTree = root.createInstance();
			root.split(leftTree, rightTree);

			// add new elements to the index elements list and set new root
			root = newRoot;
			getIndexElements().put(newRoot.getPageNumber(), newRoot);
			getIndexElements().put(leftTree.getPageNumber(), leftTree);
			getIndexElements().put(rightTree.getPageNumber(), rightTree);

			// insert record in one of the splitted trees
			if (key.compareTo(leftTree.getMaxKey()) <= 0)
				inserted = leftTree.insert(key, record);
			else
				inserted = rightTree.insert(key, record);

			// pull center record to parent and update the record which was at this position
			// before
			AbstractRecord leftRecord = getNodeRecPrototype().clone();
			leftRecord.setValue(0, leftTree.getMaxKey());
			leftRecord.setValue(1, new SQLInteger(leftTree.getPageNumber()));

			AbstractRecord rightRecord = getNodeRecPrototype().clone();
			rightRecord.setValue(0, rightTree.getMaxKey());
			rightRecord.setValue(1, new SQLInteger(rightTree.getPageNumber()));

			newRoot.getIndexPage().insert(leftRecord);
			newRoot.getIndexPage().insert(rightRecord);

			return inserted;
		}
	}

	/**
	 * @param key
	 *            key of the record to find
	 * @return if found, returns the record, otherwise null
	 */
	@Override
	public AbstractRecord lookup(T key) {
		// TODO: implement this method
		// Call lookup on root, everything else gets handled by node or leaf lookup
		return root.lookup(key);
	}

}
