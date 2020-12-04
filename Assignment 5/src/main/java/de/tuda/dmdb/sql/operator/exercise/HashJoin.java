package de.tuda.dmdb.sql.operator.exercise;

import java.util.HashMap;

import de.tuda.dmdb.sql.operator.HashJoinBase;
import de.tuda.dmdb.sql.operator.Operator;
import de.tuda.dmdb.storage.AbstractRecord;

public class HashJoin extends HashJoinBase {

	public HashJoin(Operator leftChild, Operator rightChild, int leftAtt, int rightAtt) {
		super(leftChild, rightChild, leftAtt, rightAtt);
	}

	@Override
	public void open() {
		// TODO: implement this method
		// open children
		leftChild.open();
		rightChild.open();
		// build hashmap
		hashMap = new HashMap<>();
		AbstractRecord record = null;
		while ((record = leftChild.next()) != null)
			hashMap.put(record.getValue(leftAtt), record);
	}

	@Override
	public AbstractRecord next() {
		// TODO: implement this method

		// probe HashTable and return next record
		AbstractRecord record = null;
		while ((record = rightChild.next()) != null) {
			// if records can be joined, append and return them
			if (hashMap.containsKey(record.getValue(rightAtt)))
				return hashMap.get(record.getValue(rightAtt)).append(record);
		}

		return null;
	}

	@Override
	public void close() {
		// TODO: implement this method
		// close children
		leftChild.close();
		rightChild.close();
	}
}
