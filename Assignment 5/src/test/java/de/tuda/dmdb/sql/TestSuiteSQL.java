package de.tuda.dmdb.sql;

import junit.framework.Test;
import junit.framework.TestSuite;

public class TestSuiteSQL extends TestSuite
{
  public static Test suite()
  {
    TestSuite suite = new TestSuite( "DMDB-SQL" );
    suite.addTestSuite( TestHashJoin.class );
    suite.addTestSuite( TestShuffle.class );
    return suite;
  }
}
