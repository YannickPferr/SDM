package de.tuda.dmdb;

import de.tuda.dmdb.catalog.CatalogManager;

public abstract class TestCase extends junit.framework.TestCase {

	public TestCase() {
	}

	public TestCase(String name) {
		super(name);
	}

	public void setUp(){	
		CatalogManager.clear();
	}
}
