package de.tuda.dmdb.access;

import java.util.List;

import org.junit.Assert;

import de.tuda.dmdb.TestCase;
import de.tuda.dmdb.access.exercise.HeapTable;
import de.tuda.dmdb.access.exercise.RangeEncodedBitmapIndex;
import de.tuda.dmdb.storage.AbstractRecord;
import de.tuda.dmdb.storage.Record;
import de.tuda.dmdb.storage.types.exercise.SQLInteger;
import de.tuda.dmdb.storage.types.exercise.SQLVarchar;

public class TestRangeEncodedBitmapIndex extends TestCase{

	/**
	 * Insert four records and reads them again using a SQLInteger index
	 */
	public void testRangeLookupSimple(){
		AbstractRecord record1 = new Record(2);
		record1.setValue(0, new SQLInteger(1));
		record1.setValue(1, new SQLVarchar("Hello111", 10));
		
		AbstractRecord record2 = new Record(2);
		record2.setValue(0, new SQLInteger(2));
		record2.setValue(1, new SQLVarchar("Hello112", 10));
		
		AbstractRecord record3 = new Record(2);
		record3.setValue(0, new SQLInteger(3));
		record3.setValue(1, new SQLVarchar("Hello113", 10));
		
		AbstractRecord record4 = new Record(2);
		record4.setValue(0, new SQLInteger(4));
		record4.setValue(1, new SQLVarchar("Hello114", 10));
		
		AbstractTable table = new HeapTable(record1.clone());

		table.insert(record1);
		table.insert(record2);
		table.insert(record3);
		table.insert(record3);
		
		AbstractBitmapIndex<SQLInteger> index = new RangeEncodedBitmapIndex<SQLInteger>(table, 0);
//		index.print();
		
		List<AbstractRecord>  result = index.rangeLookup((SQLInteger) record1.getValue(0), (SQLInteger) record2.getValue(0));
		Assert.assertEquals(2, result.size());
		Assert.assertEquals(record1, result.get(0));
		Assert.assertEquals(record2, result.get(1));
		
		result = index.rangeLookup((SQLInteger) record3.getValue(0), (SQLInteger) record4.getValue(0));
		Assert.assertEquals(2, result.size());
		Assert.assertEquals(record3, result.get(0));
		Assert.assertEquals(record3, result.get(1));
		
	}
}
