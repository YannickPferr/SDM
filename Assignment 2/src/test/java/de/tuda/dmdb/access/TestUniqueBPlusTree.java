package de.tuda.dmdb.access;

import org.junit.Assert;

import de.tuda.dmdb.access.exercise.HeapTable;
import de.tuda.dmdb.access.exercise.UniqueBPlusTree;
import de.tuda.dmdb.storage.AbstractRecord;
import de.tuda.dmdb.storage.Record;
import de.tuda.dmdb.storage.types.exercise.SQLInteger;
import de.tuda.dmdb.storage.types.exercise.SQLVarchar;
import de.tuda.dmdb.TestCase;

public class TestUniqueBPlusTree extends TestCase{
	
	/**
	 * Insert three records and reads them again using a SQLInteger index
	 */
	public void testIndexSimple(){
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
		
		AbstractUniqueIndex<SQLInteger> index = new UniqueBPlusTree<SQLInteger>(table, 0, 1);
		index.insert(record1);
		index.insert(record2);
		index.insert(record3);
		index.insert(record4);
//		index.print();
		
		AbstractRecord record1Cmp = index.lookup((SQLInteger) record1.getValue(0));
		Assert.assertEquals(record1, record1Cmp);
		
		AbstractRecord record2Cmp = index.lookup((SQLInteger) record2.getValue(0));
		Assert.assertEquals(record2, record2Cmp);
		
		AbstractRecord record3Cmp = index.lookup((SQLInteger) record3.getValue(0));
		Assert.assertEquals(record3, record3Cmp);
	}
	
	/**
	 * Insert three records and reads them again using a SQLVarchar index
	 */
	public void testIndexSimple2(){
		AbstractRecord record1 = new Record(2);
		record1.setValue(0, new SQLInteger(1));
		record1.setValue(1, new SQLVarchar("Hello111", 10));
		
		AbstractRecord record2 = new Record(2);
		record2.setValue(0, new SQLInteger(2));
		record2.setValue(1, new SQLVarchar("Hello112", 10));
		
		AbstractRecord record3 = new Record(2);
		record3.setValue(0, new SQLInteger(3));
		record3.setValue(1, new SQLVarchar("Hello113", 10));
		
		AbstractTable table = new HeapTable(record1.clone());
		
		AbstractUniqueIndex<SQLVarchar> index = new UniqueBPlusTree<SQLVarchar>(table, 1, 1);
		index.insert(record1);
		index.insert(record2);
		index.insert(record3);
//		index.print();
		
		AbstractRecord record1Cmp = index.lookup((SQLVarchar) record1.getValue(1));
		Assert.assertEquals(record1, record1Cmp);
		
		AbstractRecord record2Cmp = index.lookup((SQLVarchar) record2.getValue(1));
		Assert.assertEquals(record2, record2Cmp);
		
		AbstractRecord record3Cmp = index.lookup((SQLVarchar) record3.getValue(1));
		Assert.assertEquals(record3, record3Cmp);
	}
	
}
