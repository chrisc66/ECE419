package testing;

import java.io.IOException;
import org.apache.log4j.Level;
import junit.framework.Test;
import junit.framework.TestSuite;
import logger.LogSetup;

public class AllTests {

	static {
		try {
			new LogSetup("logs/testing/test.log", Level.OFF);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static Test suite() {
		
		TestSuite clientSuite = new TestSuite("Basic Storage ServerTest-Suite");
		
		// Non-distributed Storage Server Tests
		clientSuite.addTestSuite(ConnectionTest.class);				// Non-distributed
		clientSuite.addTestSuite(InteractionTest.class);			// Non-distributed
		clientSuite.addTestSuite(AdditionalTest.class);				// Non-distributed
		
		// Distributed Storage Server Tests
		clientSuite.addTestSuite(ECSBasicTests.class);				// Distributed
		clientSuite.addTestSuite(ECSReplicationTest.class);			// Distributed
		clientSuite.addTestSuite(ECSConsistentHashRingTest.class);	// Distributed
		clientSuite.addTestSuite(ECSCornerCasesTest.class);			// Distributed

		// Commenting out performance test to save some time when running tests 
		// clientSuite.addTestSuite(PerformanceTest.class);			// Performance
		// clientSuite.addTestSuite(ECSPerformanceTest.class);		// Performance

		return clientSuite;
	}
	
}
