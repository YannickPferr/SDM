package de.tuda.dmdb.access;

import junit.framework.Test;
import junit.framework.TestSuite;

public class TestSuiteAccess extends TestSuite
{
  public static Test suite()
  {
    TestSuite suite = new TestSuite( "DMDB-Access" );
    suite.addTestSuite( TestApproximateBitmapIndex.class );
    suite.addTestSuite( TestNaiveBitmapIndex.class );
    suite.addTestSuite( TestRangeEncodedBitmapIndex.class );
    return suite;
  }
}
