package de.tuda.dmdb.storage.exercise;

import java.util.Arrays;

import de.tuda.dmdb.storage.AbstractPage;
import de.tuda.dmdb.storage.AbstractRecord;
import de.tuda.dmdb.storage.types.AbstractSQLValue;
import de.tuda.dmdb.storage.types.exercise.SQLInteger;

public class RowPage extends AbstractPage {

	/**
	 * Constructir for a row page with a given (fixed) slot size
	 * 
	 * @param slotSize
	 */
	public RowPage(int slotSize) {
		super(slotSize);
	}

	@Override
	// Inserts a record at the specified slot-number. flag doInsert=true should
	// insert while shifting
	// existing records, otherwise an in-place update should occur.
	// Exception is thrown if no space left (same as in insert(AbstractRecord))
	public void insert(int slotNumber, AbstractRecord record, boolean doInsert) {
		// TODO: implement this method
		
		//Check if slotNumber respects the current bounds, if not throw IllegalArgumentException
		if(slotNumber > numRecords || slotNumber < 0)
			throw new IllegalArgumentException("Slot number can not be smaller than zero or greater than number of records in the page!");
		//Check if record fits into page, if not throw RuntimeException
		else if (!recordFitsIntoPage(record))
			throw new RuntimeException("No space left for record!");
		else {
			//calculate offset where record should be inserted
			int offset = slotNumber * slotSize;

			// Shift to make space for new record if doInsert flag is set and slotNr isnt the next free slot
			if (doInsert && slotNumber != numRecords) 
				System.arraycopy(data, offset, data, offset + slotSize, this.offset - offset);

			for (AbstractSQLValue value : record.getValues()) {

				//Serialize every value in the record
				byte[] bytes = value.serialize();

				//If value is fixed length just store it in the next free slot
				if (value.isFixedLength()) {
					for (byte b : bytes)
						data[offset++] = b;
				} else {
					//If value is not fixed length store a pointer to the position of the varchar and its size in the next free slot 
					//and store the varchar at the next free spot starting from the end of the page
					
					// Ptr to varchar
					SQLInteger startPos = new SQLInteger(offsetEnd - (bytes.length - 1));
					SQLInteger size = new SQLInteger(bytes.length);

					//store in next free slot
					for (byte b : startPos.serialize())
						data[offset++] = b;
					for (byte b : size.serialize())
						data[offset++] = b;

					//store varchar at next free spot from the end of the page
					for (int i = bytes.length - 1; i >= 0; i--)
						data[offsetEnd--] = bytes[i];
				}
			}

			//if new record war inserted, increment the record count and change offset value
			if (doInsert) 
				offset = numRecords++ * slotSize;
		}
	}

	@Override
	// Inserts a record at the end of the current page and updates the slot-size if
	// there is still space left,
	// otherwise throws an exception
	public int insert(AbstractRecord record) {
		// TODO: implement this method
		for (AbstractSQLValue value : record.getValues()) {

			//Serialize every value in the record
			byte[] bytes = value.serialize();

			//If value is fixed length just store it in the next free slot
			if (value.isFixedLength()) {
				for (byte b : bytes)
					data[offset++] = b;
			} else {
				//If value is not fixed length store a pointer to the position of the varchar and its size in the next free slot 
				//and store the varchar at the next free spot starting from the end of the page
				
				// Ptr to varchar
				SQLInteger startPos = new SQLInteger(offsetEnd - (bytes.length - 1));
				SQLInteger size = new SQLInteger(bytes.length);

				//store in next free slot
				for (byte b : startPos.serialize())
					data[offset++] = b;
				for (byte b : size.serialize())
					data[offset++] = b;

				//store varchar at next free spot from the end of the page
				for (int i = bytes.length - 1; i >= 0; i--)
					data[offsetEnd--] = bytes[i];
			}
		}

		//if new record war inserted, increment the record count	
		return numRecords++;
	}

	@Override
	// Fills the passed record-reference with values from the Page. (The
	// record-reference specifies the SQL-datatypes).
	// An Exception is thrown if the specified slot is empty.
	public void read(int slotNumber, AbstractRecord record) {
		// TODO: implement this method
		if (slotNumber >= numRecords)
			throw new IllegalArgumentException("No record found at specified slot number!");

		//Calculate offset where slot should be read from
		int offset = slotNumber * slotSize;

		int i = 0;
		for (AbstractSQLValue value : record.getValues()) {
			
			if (value.isFixedLength()) {
				//if value is fixed length, read the slot starting from offset and deserialize the data
				value.deserialize(Arrays.copyOfRange(data, offset, offset + value.getFixedLength()));
				offset += value.getFixedLength();
			} else {
				//if value is variable length, first read the pointer starting from offset
				SQLInteger startPos = new SQLInteger();
				startPos.deserialize(Arrays.copyOfRange(data, offset, offset + startPos.getFixedLength()));

				offset += startPos.getFixedLength();

				SQLInteger size = new SQLInteger();
				size.deserialize(Arrays.copyOfRange(data, offset, offset + size.getFixedLength()));

				offset += size.getFixedLength();

				//second, read the varchar starting from the offset value to the size value specified in the pointer read before 
				value.deserialize(Arrays.copyOfRange(data, startPos.getValue(), startPos.getValue() + size.getValue()));
			}
			record.setValue(i++, value);
		}
	}
}
