package testing;

import java.io.IOException;
import org.apache.log4j.Level;
import junit.framework.Test;
import junit.framework.TestSuite;
import logger.LogSetup;

public class AllTests {

	static {
		try {
			new LogSetup("logs/testing/test.log", Level.ERROR);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static Test suite() {
		
		TestSuite clientSuite = new TestSuite("Basic Storage ServerTest-Suite");
		
		// Non-distributed Storage Server Tests
		clientSuite.addTestSuite(ConnectionTest.class);
		clientSuite.addTestSuite(InteractionTest.class);
		clientSuite.addTestSuite(AdditionalTest.class);
		
		// Distributed Storage Server Tests
		clientSuite.addTestSuite(ECSBasicTests.class);
		clientSuite.addTestSuite(ECSConsistentHashRingTest.class);
		clientSuite.addTestSuite(ECSCornerCasesTest.class);

		// Commenting out performance test to save some time when running tests 
		// clientSuite.addTestSuite(PerformanceTest.class);
		// clientSuite.addTestSuite(ECSPerformanceTest.class);

		return clientSuite;
	}
	
}
