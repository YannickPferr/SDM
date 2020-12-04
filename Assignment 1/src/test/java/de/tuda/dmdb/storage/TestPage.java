package de.tuda.dmdb.storage;

import org.junit.Assert;

import de.tuda.dmdb.storage.exercise.RowPage;
import de.tuda.dmdb.storage.types.exercise.SQLInteger;
import de.tuda.dmdb.storage.types.exercise.SQLVarchar;
import de.tuda.dmdb.TestCase;


public class TestPage extends TestCase{
	public void testInsert1Record(){
		//insert record
		AbstractRecord r1 = new Record(2);
		r1.setValue(0, new SQLInteger(123456789));
		r1.setValue(1, new SQLVarchar("Test", 10));
		AbstractPage p = new RowPage(r1.getFixedLength());
		p.insert(r1);

		//read record
		AbstractRecord r1cmp = new Record(2);
		r1cmp.setValue(0, new SQLInteger());
		r1cmp.setValue(1, new SQLVarchar(10));
		p.read(0, r1cmp);

		//compare
		Assert.assertEquals(r1, r1cmp);
	}
}
