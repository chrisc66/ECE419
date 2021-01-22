package app_kvClient;

import client.KVCommInterface;
import client.KVStore;
import org.apache.log4j.Level;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.UnknownHostException;

import logger.LogSetup;

public class KVClient implements IKVClient {

    private KVStore kvStore = null;

    private boolean stop = false;
    private BufferedReader stdin;
    private static final String PROMPT = "Client> ";
    private String serverAddress;
    private int serverPort;

    @Override
    public void newConnection(String hostname, int port) throws Exception{
        // TODO Auto-generated method stub
        kvStore = new KVStore(hostname, port);
        kvStore.connect();
    }

    @Override
    public KVCommInterface getStore(){
        // TODO Auto-generated method stub
        return kvStore;
    }

    public void run() throws Exception{
        while(!stop) {
            stdin = new BufferedReader(new InputStreamReader(System.in));
            System.out.print(PROMPT);

            try {
                String cmdLine = stdin.readLine();
                this.handleCommand(cmdLine);
            } catch (IOException e) {
                stop = true;
                printError("CLI does not respond - Application terminated ");
            }
        }
    }

    private void handleCommand(String cmdLine) throws Exception{
        String[] tokens = cmdLine.split("\\s+");
        //!!!!!!!!!!! need to check if kvStore == null
        if(tokens[0].equals("quit")) {
            stop = true;
            kvStore.disconnect();
            System.out.println(PROMPT + "Application exit!");

        } else if (tokens[0].equals("connect")){
            if(tokens.length == 3) {
                try{
                    serverAddress = tokens[1];
                    serverPort = Integer.parseInt(tokens[2]);
                    newConnection(serverAddress, serverPort);
                } catch(NumberFormatException nfe) {
                    printError("No valid address. Port must be a number!");
//                    logger.info("Unable to parse argument <port>", nfe);
                } catch (UnknownHostException e) {
                    printError("Unknown Host!");
//                    logger.info("Unknown Host!", e);
                } catch (IOException e) {
                    printError("Could not establish connection!");
//                    logger.warn("Could not establish connection!", e);
                }
            } else {
                printError("Invalid number of parameters!");
            }

        } else  if (tokens[0].equals("put")) {
            if(tokens.length >= 2) {
                if(kvStore != null && kvStore.isRunning()){
                    StringBuilder value = new StringBuilder();
                    String key = tokens[1];
                    for(int i = 2; i < tokens.length; i++) {
                        value.append(tokens[i]);
                        if (i != tokens.length -1 ) {
                            value.append(" ");
                        }
                    }
                    //!!!!! need to check key value size
                    // byte[] utf16Bytes= string.getBytes("UTF-16BE");
                    // size = System.out.println(utf16Bytes.length);
                    kvStore.put(key, value.toString());
                } else {
                    printError("Not connected!");
                }
            } else {
                printError("No message passed!");
            }

        } else if(tokens[0].equals("disconnect")) {
            kvStore.disconnect();

        } else if(tokens[0].equals("logLevel")) {
            // need to implement logger
//            if(tokens.length == 2) {
//                String level = setLevel(tokens[1]);
//                if(level.equals(LogSetup.UNKNOWN_LEVEL)) {
//                    printError("No valid log level!");
//                    printPossibleLogLevels();
//                } else {
//                    System.out.println(PROMPT +
//                            "Log level changed to level " + level);
//                }
//            } else {
//                printError("Invalid number of parameters!");
//            }

        } else if(tokens[0].equals("help")) {
            //!!!!!! may need to implement printHelp()
//            printHelp();
        } else {
            printError("Unknown command");
//            printHelp();
        }
    }

    private void printError(String error){
        System.out.println(PROMPT + "Error! " +  error);
    }

    public static void main(String[] args) throws Exception{
        try {
//            new LogSetup("logs/client.log", Level.OFF);
            KVClient app = new KVClient();
            app.run();
        } catch (IOException e) {
            System.out.println("Error! Unable to initialize logger!");
            e.printStackTrace();
            System.exit(1);
        }
    }
}
