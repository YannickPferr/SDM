package de.tuda.dmdb.access;

import junit.framework.Test;
import junit.framework.TestSuite;

public class TestSuiteAccess extends TestSuite
{
  public static Test suite()
  {
    TestSuite suite = new TestSuite( "DMDB-Access" );
    suite.addTestSuite( TestUniqueBPlusTree.class );
    return suite;
  }
}
