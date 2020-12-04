package de.tuda.dmdb.sql.operator;

import java.util.HashMap;

import de.tuda.dmdb.storage.AbstractRecord;
import de.tuda.dmdb.storage.types.AbstractSQLValue;

public abstract class HashJoinBase extends BinaryOperator {
	protected int leftAtt=0;
	protected int rightAtt=0;
	protected HashMap<AbstractSQLValue, AbstractRecord> hashMap;
	
	protected AbstractRecord leftRecord = null;
	
	public HashJoinBase(Operator leftChild, Operator rightChild, int leftAtt, int rightAtt) {
		super(leftChild, rightChild);
		this.leftAtt = leftAtt;
		this.rightAtt = rightAtt;
	}
	
}
