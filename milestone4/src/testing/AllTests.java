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
		
		// M1: Non-distributed Storage Server Tests
		clientSuite.addTestSuite(ConnectionTest.class);				// server-client connection
		clientSuite.addTestSuite(InteractionTest.class);			// server-client interaction
		clientSuite.addTestSuite(AdditionalTest.class);				// server-client disk storage and communication module

		// M2 & M3: Distributed Storage Server Tests (Eventual Consistency Model)
		clientSuite.addTestSuite(ECSBasicTests.class);				// ecs nodes management and server-client interaction
		clientSuite.addTestSuite(ECSReplicationTest.class);			// multi-server data replication
		clientSuite.addTestSuite(ECSConsistentHashRingTest.class);	// ecs consistent hash ring functionality
		clientSuite.addTestSuite(ECSCornerCasesTest.class);			// ecs consistent hash ring corner cases

		// M4: Distributed Storage Server Tests (Sequencial Consistency & Data Subscription)
		clientSuite.addTestSuite(StrictConsistencyTest.class);		// distributed system stricter consistency rules
		clientSuite.addTestSuite(DataSubscriptionTest.class);		// distributed system data subscription rules

		// Perf: Commenting out performance test to save some time when running tests 
		// clientSuite.addTestSuite(PerformanceTest.class);			// performance tests for non-distributed service
		// clientSuite.addTestSuite(ECSPerformanceTest.class);		// performance tests for distributed service

		return clientSuite;
	}
	
}
