package de.tuda.dmdb.storage.types;

import org.junit.Assert;

import de.tuda.dmdb.storage.types.exercise.SQLInteger;
import de.tuda.dmdb.TestCase;


public class TestSQLInteger extends TestCase {
	public void testSerializeDeserialize1(){
		int value = 123456789;
		
		SQLInteger sqlInt = new SQLInteger(value);
		byte[] content = sqlInt.serialize();
		
		SQLInteger sqlInt2 = new SQLInteger();
		sqlInt2.deserialize(content);
		
		Assert.assertEquals(sqlInt.getValue(), sqlInt2.getValue());
	}
}
