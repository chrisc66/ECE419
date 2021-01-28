package app_kvClient;

import client.KVCommInterface;
import client.KVStore;
import logger.LogSetup;
import shared.messages.KVMessage;
import shared.messages.KVMessageClass;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.UnknownHostException;

public class KVClient implements IKVClient {

    private static final String PROMPT = "Client> ";
    private static Logger logger = Logger.getRootLogger();

    private KVStore kvStore = null;
    private boolean stop = false;
    private BufferedReader stdin;
    private String serverAddress;
    private int serverPort;

    @Override
    public void newConnection(String hostname, int port) throws Exception{
        try {
            kvStore = new KVStore(hostname, port);
            kvStore.connect();
        }
        catch (Exception e){
            printError("The socket cannot be initialized! ");
        }
    }

    @Override
    public KVCommInterface getStore(){
        return kvStore;
    }

    public void run() throws Exception{
        while(!stop) {
            stdin = new BufferedReader(new InputStreamReader(System.in));
            System.out.print(PROMPT);

            try {
                String cmdLine = stdin.readLine();
                this.handleCommand(cmdLine);
            } 
            catch (IOException e) {
                stop = true;
                printError("No response - Application terminated ");
            }
        }
    }

    private void handleCommand(String cmdLine) throws Exception{
        String[] tokens = cmdLine.split("\\s+");
        if(tokens[0].equals("quit")) {
            stop = true;
            if(kvStore != null){
                kvStore.disconnect();
            }
            System.out.println("Application exit!");
        } 
        else if (tokens[0].equals("connect")){
            if(tokens.length == 3) {
                try{
                    serverAddress = tokens[1];
                    serverPort = Integer.parseInt(tokens[2]);
                    newConnection(serverAddress, serverPort);
                } 
                catch(NumberFormatException nfe) {
                    printError("No valid address. Port must be a number!");
                    logger.info("Unable to parse argument <port>", nfe);
                } 
                catch (UnknownHostException e) {
                    printError("Unknown Host!");
                    logger.info("Unknown Host!", e);
                } 
                catch (IOException e) {
                    printError("Could not establish connection!");
                    logger.error("Could not establish connection!", e);
                }
            } 
            else {
                printError("Invalid number of parameters!");
            }

        } 
        else if (tokens[0].equals("put")) {
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
                    kvStore.put(key, value.toString());
                } 
                else {
                    printError("Not connected!");
                }
            } 
            else {
                printError("No message passed!");
            }
        } 
        else if (tokens[0].equals("get")) {
            if(tokens.length >= 1) {
                if(kvStore != null && kvStore.isRunning()){
                    String key = tokens[1];
                    KVMessage msg = kvStore.get(key);
                    printOutput("Key: " + msg.getKey() + ", Value: " + msg.getValue());
                } 
                else {
                    printError("Not connected!");
                }
            } 
            else {
                printError("No message passed!");
            }
        } 
        else if(tokens[0].equals("disconnect")) {
            if(kvStore != null){
                kvStore.disconnect();
            }
        } 
        else if(tokens[0].equals("logLevel")) {
            if(tokens.length == 2) {
                String level = setLevel(tokens[1]);
                if(level.equals(LogSetup.UNKNOWN_LEVEL)) {
                    printError("No valid log level!");
                    printPossibleLogLevels();
                } 
                else {
                    logger.info("Log level changed to level " + level);
                }
            } 
            else {
                printError("Invalid number of parameters!");
            }

        } 
        else if(tokens[0].equals("help")) {
            printHelp();
        } 
        else {
            printError("Unknown command, see help");
        }
    }

    private void printError(String error){
        System.out.println("Error! " +  error);
    }

    private void printOutput(String out){
        System.out.println(out);
    }

    private void printPossibleLogLevels() {
        System.out.println("Possible log levels are:");
        System.out.println(LogSetup.getPossibleLogLevels());
    }

    private String setLevel(String levelString) {

        if(levelString.equals(Level.ALL.toString())) {
            logger.setLevel(Level.ALL);
            return Level.ALL.toString();
        } 
        else if(levelString.equals(Level.DEBUG.toString())) {
            logger.setLevel(Level.DEBUG);
            return Level.DEBUG.toString();
        } 
        else if(levelString.equals(Level.INFO.toString())) {
            logger.setLevel(Level.INFO);
            return Level.INFO.toString();
        } 
        else if(levelString.equals(Level.WARN.toString())) {
            logger.setLevel(Level.WARN);
            return Level.WARN.toString();
        } 
        else if(levelString.equals(Level.ERROR.toString())) {
            logger.setLevel(Level.ERROR);
            return Level.ERROR.toString();
        } 
        else if(levelString.equals(Level.FATAL.toString())) {
            logger.setLevel(Level.FATAL);
            return Level.FATAL.toString();
        } 
        else if(levelString.equals(Level.OFF.toString())) {
            logger.setLevel(Level.OFF);
            return Level.OFF.toString();
        } 
        else {
            return LogSetup.UNKNOWN_LEVEL;
        }
    }

    private void printHelp(){
        System.out.println("Possible commands are:");
        System.out.println("    connect <ip> <port>");
        System.out.println("    disconnect");
        System.out.println("    quit");
        System.out.println("    help");
        System.out.println("    logLevel <level>");
        System.out.println("    put <key> <value>");
        System.out.println("    get <key>");
    }

    public static void main(String[] args) throws Exception{
        try {
            new LogSetup("logs/client.log", Level.ALL);
            KVClient app = new KVClient();
            app.run();
        } 
        catch (IOException e) {
            logger.error("Error! Unable to initialize client logger!", e);
			e.printStackTrace();
			System.exit(1);
        }
    }
}
