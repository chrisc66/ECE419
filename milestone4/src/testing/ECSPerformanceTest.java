package testing;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import client.KVStore;
import app_kvECS.*;

import junit.framework.TestCase;

import java.util.List;
import java.nio.charset.StandardCharsets;
import java.util.Random;

public class ECSPerformanceTest extends TestCase {

    // ECS Client
    // private static final String configFilePath = System.getProperty("user.dir") + "/ecs.config";
    private static final String configFilePath = "/Users/Zichun.Chong@ibm.com/Desktop/ece419/project/milestone4/ecs.config";
    private ECSClient ecs;
    // KVServer
    private static final int numKvServer = 8;
    private static final String cacheStrategy = "NONE";
    private static final int cacheSize = 0;
    // KVClient
    private KVStore kvClient;
    // Data
    private byte[] data_16;
    private String data_16_str;
    private String key = "foo";
    Random r = new Random(10);

    @Before
    public void setUp() {
        
        try {
            ecs = new ECSClient(configFilePath);
            ecs.ECSInitialization(0);
        	ecs.addNodes(numKvServer, cacheStrategy, cacheSize);
        	try {
            	ecs.awaitNodes(1, 2000);
        	} catch (Exception e) {}
            ecs.start();
        } catch (Exception e) {
            System.out.println("ECS Test error "+e);
        }
        
        // get one of the avaliable servers
        List<String> curServers = ecs.getCurrentServers();
        System.out.println(curServers);
        String servername = curServers.get(0);
        String[] tokens = servername.split(":");
        String hostname = tokens[0];
        int port = Integer.parseInt(tokens[1]);
        System.out.println("ECSBasicTests testPut: connecting to " + hostname + ":" + port);
        
        kvClient = new KVStore(hostname, port);

        data_16 = new byte[16];
        r.nextBytes(data_16);
        data_16_str = new String(data_16, StandardCharsets.UTF_8);

        try {
            kvClient.connect();
        } catch (Exception e) {
            System.err.println("Client Connection Failed!");
        }
    }

    @After
    public void tearDown() {
        kvClient.disconnect();
    }

    @Test
    public void testPerf_8020() {
        int iteration = 1000;
        int numBytes = 16;
        String getOutput = "";
        long startTime = System.currentTimeMillis();
        for (int i = 0; i < iteration; i++) {
            try {
                kvClient.put(key, data_16_str);
                kvClient.put(key, data_16_str);
                kvClient.put(key, data_16_str);
                kvClient.put(key, data_16_str);
                getOutput = kvClient.get(key).getValue();
            } catch (Exception e) {
                System.out.println("Performance test error!");
            }
        }

        long duration = System.currentTimeMillis() - startTime;

        double totalBytes = iteration * (4 * numBytes + getOutput.getBytes(StandardCharsets.UTF_8).length);
        double perf = 1000.0 * totalBytes / (1024 * duration);
        double latency = 1000 * duration / (iteration * 5);

        System.out.println("Perf (80% put & 20% get)    = " + perf + " KB/s");
        System.out.println("Latency (80% put & 20% get) = " + latency + " ms");
    }

    @Test
    public void testPerf_5050() {
        int iteration = 1000;
        int numBytes = 16;
        String getOutput = "";
        long startTime = System.currentTimeMillis();
        for (int i = 0; i < iteration; i++) {
            try {
                kvClient.put(key, data_16_str);
                getOutput = kvClient.get(key).getValue();
            } catch (Exception e) {
                System.out.println("Performance test error!");
            }
        }

        long duration = System.currentTimeMillis() - startTime;

        double totalBytes = iteration * (numBytes + getOutput.getBytes(StandardCharsets.UTF_8).length);
        double perf = 1000.0 * totalBytes / (1024 * duration);
        double latency = 1000 * duration / (iteration * 2);

        System.out.println("Perf (50% put & 50% get)    = " + perf + " KB/s");
        System.out.println("Latency (50% put & 50% get) = " + latency + " ms");
    }

    @Test
    public void testPerf_2080() {
        int iteration = 1000;
        int numBytes = 16;
        String getOutput = "";
        long startTime = System.currentTimeMillis();
        for (int i = 0; i < iteration; i++) {
            try {
                kvClient.put(key, data_16_str);

                getOutput = kvClient.get(key).getValue();
                getOutput = kvClient.get(key).getValue();
                getOutput = kvClient.get(key).getValue();
                getOutput = kvClient.get(key).getValue();
            } catch (Exception e) {
                System.out.println("Performance test error!");
            }
        }

        long duration = System.currentTimeMillis() - startTime;

        double totalBytes = iteration * (numBytes + 4 * getOutput.getBytes(StandardCharsets.UTF_8).length);

        double perf = 1000.0 * totalBytes / (1024 * duration);
        double latency = 1000 * duration / (iteration * 5.0);

        System.out.println("Perf (20% put & 80% get)    = " + perf + " KB/s");
        System.out.println("Latency (20% put & 80% get) = " + latency + " ms");
    }

    @Test
    public void testPerf_1000() {
        int iteration = 1000;
        int numBytes = 16;
        long startTime = System.currentTimeMillis();
        for (int i = 0; i < iteration; i++) {
            try {
                kvClient.put(key, data_16_str);
                kvClient.put(key, data_16_str);
                kvClient.put(key, data_16_str);
                kvClient.put(key, data_16_str);
                kvClient.put(key, data_16_str);
            } catch (Exception e) {
                System.out.println("Performance test error!");
            }
        }

        long duration = System.currentTimeMillis() - startTime;

        double totalBytes = iteration * numBytes *5;
        double perf = 1000.0 * totalBytes / (1024 * duration);
        double latency = 1000 * duration / (iteration * 5);

        System.out.println("Perf (100% put & 0% get)    = " + perf + " KB/s");
        System.out.println("Latency (100% put & 0% get) = " + latency + " ms");
    }

    @Test
    public void testPerf_0100() {
        int iteration = 1000;
        // int numBytes = 16;
        String getOutput = "";
        try {
            kvClient.put(key, data_16_str);
        } catch (Exception e) {
            System.out.println("Performance test error!");
        }
        long startTime = System.currentTimeMillis();
        for (int i = 0; i < iteration; i++) {
            try {
                getOutput = kvClient.get(key).getValue();
                getOutput = kvClient.get(key).getValue();
                getOutput = kvClient.get(key).getValue();
                getOutput = kvClient.get(key).getValue();
                getOutput = kvClient.get(key).getValue();
            } catch (Exception e) {
                System.out.println("Performance test error!");
            }
        }

        long duration = System.currentTimeMillis() - startTime;

        double totalBytes = iteration * getOutput.getBytes(StandardCharsets.UTF_8).length * 5;
        double perf = 1000.0 * totalBytes / (1024 * duration);
        double latency = 1000 * duration / (iteration * 5);

        System.out.println("Perf (0% put & 100% get)    = " + perf + " KB/s");
        System.out.println("Latency (0% put & 100% get) = " + latency + " ms");
    }


//------------------------------------compare between different payload (same iteration)----------------------------------------//

    @Test
    public void testPerf_put_16_() {
        int iteration = 1000;
        int numBytes = 16;
        long startTime = System.currentTimeMillis();
        for (int i = 0; i < iteration; i++) {
            try {
                kvClient.put(key, data_16_str);
            } catch (Exception e) {
                System.out.println("Performance test error!");
            }
        }

        long duration = System.currentTimeMillis() - startTime;

        double totalBytes = iteration * (numBytes);
        double perf = 1000.0 * totalBytes / (1024 * duration);
        double latency = 1000 * duration / (iteration * 1);

        System.out.println("Perf (put payload 16)    = " + perf + " KB/s");
        System.out.println("Latency (put payload 16) = " + latency + " ms");
    }

    @Test
    public void testPerf_get_16_() {
        int iteration = 1000;
        int numBytes = 16;
        try {
            kvClient.put(key, data_16_str);
        } catch (Exception e) {
            System.out.println("Performance test error!");
        }
        long startTime = System.currentTimeMillis();
        for (int i = 0; i < iteration; i++) {
            try {
                kvClient.get(key).getValue();
            } catch (Exception e) {
                System.out.println("Performance test error!");
            }
        }

        long duration = System.currentTimeMillis() - startTime;

        double totalBytes = iteration * (numBytes);
        double perf = 1000.0 * totalBytes / (1024 * duration);
        double latency = 1000 * duration / (iteration * 1);

        System.out.println("Perf (get payload 16)    = " + perf + " KB/s");
        System.out.println("Latency (get payload 16) = " + latency + " ms");
    }

}

