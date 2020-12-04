package de.tuda.dmdb.access.exercise;

import de.tuda.dmdb.access.AbstractIndexElement;
import de.tuda.dmdb.access.NodeBase;
import de.tuda.dmdb.access.UniqueBPlusTreeBase;
import de.tuda.dmdb.storage.AbstractRecord;
import de.tuda.dmdb.storage.types.AbstractSQLValue;
import de.tuda.dmdb.storage.types.exercise.SQLInteger;

/**
 * Index node
 * 
 * @author cbinnig
 *
 */
public class Node<T extends AbstractSQLValue> extends NodeBase<T> {

	/**
	 * Node constructor
	 * 
	 * @param uniqueBPlusTree
	 *            TODO
	 */
	public Node(UniqueBPlusTreeBase<T> uniqueBPlusTree) {
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

		AbstractRecord rid = uniqueBPlusTree.getNodeRecPrototype().clone();
		int pos = binarySearch(key);

		if (pos >= indexPage.getNumRecords())
			return null;

		// lookup index at position pos and read the page number
		indexPage.read(pos, rid);
		AbstractSQLValue page = rid.getValue(UniqueBPlusTreeBase.PAGE_POS);

		// recursively call lookup method (leaf is anchor of recursion)
		return uniqueBPlusTree.getIndexElement(((SQLInteger) page).getValue()).lookup(key);
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

		// recursively call insert on correct child node/leaf
		AbstractRecord child = uniqueBPlusTree.getNodeRecPrototype().clone();
		int indexToInsert = binarySearch(key);
		indexPage.read(indexToInsert, child);

		boolean inserted = uniqueBPlusTree
				.getIndexElement(((SQLInteger) child.getValue(UniqueBPlusTree.PAGE_POS)).getValue())
				.insert(key, record);
		// if insertion did work on child return true
		if (inserted)
			return true;
		else {
			// return false if leaf is full to propagate split upwards
			if (isFull())
				return false;

			// if node is not full yet, split child and pull center element to this node

			AbstractIndexElement<T> childNode = uniqueBPlusTree
					.getIndexElement(((SQLInteger) child.getValue(UniqueBPlusTree.PAGE_POS)).getValue());

			// split child
			AbstractIndexElement<T> leftTree = childNode.createInstance();
			AbstractIndexElement<T> rightTree = childNode.createInstance();
			childNode.split(leftTree, rightTree);

			// add new elements to the index elements list
			uniqueBPlusTree.getIndexElements().put(leftTree.getPageNumber(), leftTree);
			uniqueBPlusTree.getIndexElements().put(rightTree.getPageNumber(), rightTree);

			// insert record in one of the splitted trees
			if (key.compareTo(leftTree.getMaxKey()) <= 0)
				inserted = leftTree.insert(key, record);
			else
				inserted = rightTree.insert(key, record);

			// pull center record to parent and update the record which was at this position
			// before
			AbstractRecord leftRecord = getUniqueBPlusTree().getNodeRecPrototype().clone();
			leftRecord.setValue(0, leftTree.getMaxKey());
			leftRecord.setValue(1, new SQLInteger(leftTree.getPageNumber()));

			AbstractRecord rightRecord = getUniqueBPlusTree().getNodeRecPrototype().clone();
			rightRecord.setValue(0, rightTree.getMaxKey());
			rightRecord.setValue(1, new SQLInteger(rightTree.getPageNumber()));

			indexPage.insert(indexToInsert, leftRecord, false);
			indexPage.insert(indexToInsert + 1, rightRecord, true);

			return inserted;
		}
	}

	@Override
	public AbstractIndexElement<T> createInstance() {
		return new Node<T>(this.uniqueBPlusTree);
	}

}