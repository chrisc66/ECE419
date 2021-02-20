package ecs;

import app_kvECS.ECSClient;
import app_kvECS.IECSClient;
import logger.LogSetup;
import org.apache.log4j.Logger;
import org.apache.log4j.Level;

import java.io.*;

public class ECSUI{

    private boolean stop = false;
    private BufferedReader stdin;

    public void run() throws Exception{
        while(!stop) {
            stdin = new BufferedReader(new InputStreamReader(System.in));

            try {
                String cmdLine = stdin.readLine();
                this.handleCommand(cmdLine);
            }
            catch (IOException e) {
                stop = true;
            }
        }
    }
    private void handleCommand(String cmdLine) throws Exception{}


}
