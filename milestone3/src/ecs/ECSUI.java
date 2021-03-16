package ecs;

import app_kvECS.ECSClient;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

public class ECSUI{

    private boolean stop = false;
    private BufferedReader stdin;
    static ECSClient ecsClient;

    public ECSUI(String arg) {
        ecsClient = new ECSClient(arg);
    }

    public void run() throws Exception{
        while(!stop) {
            stdin = new BufferedReader(new InputStreamReader(System.in));
            System.out.println("ECS UI running");
            try {
                String cmdLine = stdin.readLine();
                this.handleCommand(cmdLine);
            }
            catch (IOException e) {
                stop = true;
            }
        }
    }
    
    private void handleCommand(String cmdLine) throws Exception {
        String[] tokens = cmdLine.split("\\s+");
        if (tokens[0].equals("addNodes")) {
            if (tokens.length == 2) {
                ecsClient.addNodes(Integer.parseInt(tokens[1]), "", 0);
            }
        } else if (tokens[0].equals("addNode")) {
            ecsClient.addNode("", 0);
        } else if (tokens[0].equals("start")) {
            ecsClient.start();
        } else if (tokens[0].equals("stop")) {
            ecsClient.stop();
        } else if (tokens[0].equals("shutDown")) {
            ecsClient.shutdown();
        } else if (tokens[0].equals("removeNode")) {
            List<String> removeServerList = new ArrayList<>();
            for (int i =1; i < tokens.length; i++) {
                removeServerList.add(tokens[i]);
            }
            ecsClient.removeNodes(removeServerList, false);
        }
    }

    public static void main(String[] args) throws Exception{
        try {
            ECSUI ecsui = new ECSUI(args[0]);
            ecsui.run();
        }
        catch (Exception e){
            System.out.println("Error! ECSClient terminated!");
            e.printStackTrace();
            System.exit(1);
        }
    }

}
