package de.tuda.dmdb.sql.operator;

import java.util.Iterator;

import de.tuda.dmdb.access.AbstractTable;
import de.tuda.dmdb.storage.AbstractRecord;

@SuppressWarnings("unused")
public abstract class TableScanBase extends Operator {
	protected AbstractTable table;
	protected Iterator<AbstractRecord> tableIter;
	
	public TableScanBase(AbstractTable table){
		super();
		
		this.table = table;
	}

}
