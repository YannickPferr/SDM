package de.tuda.dmdb.sql.operator;

import de.tuda.dmdb.storage.types.AbstractSQLValue;

@SuppressWarnings("unused")
public abstract class SelectionBase extends UnaryOperator {
	protected int attribute;
	protected AbstractSQLValue constant;
	
	public SelectionBase(Operator child, int attribute, AbstractSQLValue constant) {
		super(child);
		
		this.attribute = attribute;
		this.constant = constant;
	}

}
