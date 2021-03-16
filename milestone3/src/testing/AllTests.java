package testing;

import java.io.IOException;

import org.apache.log4j.Level;

import app_kvServer.KVServer;
import junit.framework.Test;
import junit.framework.TestSuite;
import logger.LogSetup;


public class AllTests {

	static {
		try {
			new LogSetup("logs/testing/test.log", Level.ERROR);
			KVServer kvServer = new KVServer(50000, 10, "NONE");
			new Thread(kvServer).start();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	
	public static Test suite() {
		TestSuite clientSuite = new TestSuite("Basic Storage ServerTest-Suite");
		// clientSuite.addTestSuite(ConnectionTest.class);
		// clientSuite.addTestSuite(InteractionTest.class);
		// clientSuite.addTestSuite(AdditionalTest.class);
		// Commenting out performance test to save some time when running tests 
		// clientSuite.addTestSuite(PerformanceTest.class);
		clientSuite.addTestSuite(ECSBasicTests.class);
		return clientSuite;
	}
	
}
