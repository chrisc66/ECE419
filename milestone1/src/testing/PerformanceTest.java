package testing;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import client.KVStore;

import junit.framework.TestCase;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Random;

public class PerformanceTest extends TestCase {

    private KVStore kvClient;
    private byte[] data_16, data_512, data_2048, data_4096;
    private String data_16_str, data_512_str, data_2048_str, data_4096_str, data_512__str;
    Random r = new Random(10);

    @BeforeClass
    public void setUp() {
        kvClient = new KVStore("localhost", 50000);

        data_16 = new byte[16];
        data_512 = new byte[512];
        data_2048 = new byte[2048];
        data_4096 = new byte[4096];

        r.nextBytes(data_16);
        r.nextBytes(data_512);
        r.nextBytes(data_2048);
        r.nextBytes(data_4096);

        data_16_str = new String(data_16, StandardCharsets.UTF_8);
        data_512_str = new String(data_512, StandardCharsets.UTF_8);
        data_2048_str = new String(data_2048, StandardCharsets.UTF_8);
        data_4096_str = new String(data_4096, StandardCharsets.UTF_8);

        try {
            kvClient.connect();
        } catch (Exception e) {
            System.err.println("Client Connection Failed!");
        }
    }

    @AfterClass
    public void tearDown() {
        kvClient.disconnect();
    }

    @Test
    public void testPerf_8020() {
        int iteration = 100;
        int numBytes = iteration * (16 + 512 + 2048 + 4096);
        String key = "foo";
        String getOutput = "";
        long startTime = System.currentTimeMillis();
        for (int i = 0; i < iteration; i++) {
            try {
                kvClient.put(key, data_16_str);
                kvClient.put(key, data_512_str);
                kvClient.put(key, data_2048_str);
                kvClient.put(key, data_4096_str);
            } catch (Exception e) {
                System.out.println("Performance test error!");
            }
        }

        for (int i =0; i < iteration; i++) {
            try {
                getOutput = kvClient.get(key).getValue();
            } catch (Exception e) {
                System.out.println("Performance test error!");
            }
        }

        long duration = System.currentTimeMillis() - startTime;
        System.out.println(" duration = "+ duration);

        double totalBytes = iteration * (numBytes + getOutput.getBytes(StandardCharsets.UTF_8).length);
        double perf = 1000.0 * iteration * numBytes / (1024 * 1024 * duration);
        double latency = duration / (iteration * 5);

        System.out.println("Perf (80% put & 20% get) = " + perf + " MB/s");
        System.out.println("Latency (80% put & 20% get) = " + latency + " ms");
    }
}
