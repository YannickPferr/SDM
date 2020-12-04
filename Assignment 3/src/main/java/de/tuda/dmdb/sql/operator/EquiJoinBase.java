package de.tuda.dmdb.sql.operator;

import de.tuda.dmdb.storage.AbstractRecord;

public abstract class EquiJoinBase extends BinaryOperator {
	protected int leftAtt=0;
	protected int rightAtt=0;
	
	protected AbstractRecord leftRecord = null;
	
	public EquiJoinBase(Operator leftChild, Operator rightChild, int leftAtt, int rightAtt) {
		super(leftChild, rightChild);
		this.leftAtt = leftAtt;
		this.rightAtt = rightAtt;
	}
}
