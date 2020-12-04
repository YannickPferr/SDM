package de.tuda.dmdb.access.exercise;

import de.tuda.dmdb.access.HeapTableBase;
import de.tuda.dmdb.access.RecordIdentifier;
import de.tuda.dmdb.storage.AbstractPage;
import de.tuda.dmdb.storage.AbstractRecord;
import de.tuda.dmdb.storage.PageManager;

public class HeapTable extends HeapTableBase {

	/**
	 * 
	 * Constructs table from record prototype
	 * 
	 * @param prototypeRecord
	 */
	public HeapTable(AbstractRecord prototypeRecord) {
		super(prototypeRecord);
	}

	@Override
	// Inserts a record in the last Page of the Heap-Table. If no
	// space is left in a page, a new page is created.
	public RecordIdentifier insert(AbstractRecord record) {
		// TODO: implement this method
		RecordIdentifier rid = new RecordIdentifier(0, 0);

		if (lastPage.recordFitsIntoPage(record)) {
			//if the record fits into the page, the record gets inserted and the corresponding RecordIdentifier is returned
			int slotNumber = lastPage.insert(record);
			rid.setPageNumber(lastPage.getPageNumber());
			rid.setSlotNumber(slotNumber);

			return rid;
		} else {
			//if record doesnt fit into page, a new page gets created and set as the new last page, then method gets called recursively
			AbstractPage newPage = PageManager.createDefaultPage(this.prototype.getFixedLength());
			addPage(newPage);
			lastPage = newPage;
			
			//recursive method call
			return insert(record);
		}
	}

	@Override
	// Returns a record by its pageNumber & slotNumber.
	public AbstractRecord lookup(int pageNumber, int slotNumber) {
		// TODO: implement this method
		//getPage and prototype of records, then read the record from the page
		AbstractPage page = getPage(pageNumber);
		AbstractRecord record = this.prototype.clone();
		page.read(slotNumber, record);

		return record;
	}
}
