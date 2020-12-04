package de.tuda.dmdb;

import junit.framework.Test;
import junit.framework.TestSuite;
import de.tuda.dmdb.access.TestSuiteAccess;

public class TestSuiteDMDB extends TestSuite
{
  public static Test suite()
  {
    TestSuite suite = new TestSuite( "DMDB-All" );
    suite.addTest(TestSuiteAccess.suite());
    return suite;
  }
}
