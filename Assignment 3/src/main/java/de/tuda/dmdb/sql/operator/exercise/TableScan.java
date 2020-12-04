package de.tuda.dmdb.sql.operator.exercise;

import de.tuda.dmdb.access.AbstractTable;
import de.tuda.dmdb.sql.operator.TableScanBase;
import de.tuda.dmdb.storage.AbstractRecord;

@SuppressWarnings("unused")
public class TableScan extends TableScanBase {

	public TableScan(AbstractTable table) {
		super(table);
	}

	@Override
	public void open() {
		// TODO: implement this method
		// Initialize the iterator
		tableIter = table.iterator();
	}

	@Override
	public AbstractRecord next() {
		// TODO: implement this method
		//if table has another element return it, else return null
		if (tableIter.hasNext()) {
			return tableIter.next();
		}

		return null;
	}

	@Override
	public void close() {
		// TODO: implement this method
		// Reset iterator
		tableIter = null;
	}
}
