package de.tuda.dmdb;

import junit.framework.Test;
import junit.framework.TestSuite;
import de.tuda.dmdb.access.TestSuiteAccess;
import de.tuda.dmdb.storage.TestSuiteStorage;

public class TestSuiteDMDB extends TestSuite
{
  public static Test suite()
  {
    TestSuite suite = new TestSuite( "DMDB-All" );
    suite.addTest(TestSuiteAccess.suite());
    suite.addTest(TestSuiteStorage.suite());
    return suite;
  }
}
