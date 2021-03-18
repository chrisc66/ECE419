package ecs;

import org.apache.log4j.Logger;

import shared.messages.Metadata;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

/**This class contains a Hash Ring which stores the **/
public class ECSConsistantHashRing {

    /**The server names are defined as address:port**/
    private static Logger logger = Logger.getRootLogger();
    private HashMap<String,IECSNode> HashRing = new HashMap<>(); //key: BigInteger in string format
    private List<BigInteger> keyArray = new ArrayList<>();
    private int hashRingSize = 0;
    private Object ExceptionInInitializerError;
    private Object RuntimeException;

    public ECSConsistantHashRing() {}

    public void initializeHashRing(List<String> ServerNames) throws Throwable {
        if (ServerNames.isEmpty()){
            logger.error("Please reset the list of input server Names");
            return;
        }

        hashRingSize = ServerNames.size();
        for (int i = 0; i < hashRingSize; i++){
            /** Assume server name are in the format of ip:port **/
            String serverName = ServerNames.get(i);

            ECSNode newNode = getECSNodeFromName(serverName);
            HashRing.put(newNode.getCurNodeIDStart(),newNode);
            keyArray.add(new BigInteger(newNode.getCurNodeIDStart()));
        }
        Collections.sort(keyArray);

        for (int i = 0; i < hashRingSize; i++){
            BigInteger curServerKey = keyArray.get(i);
            if (!curServerKey.toString().equals(HashRing.get(curServerKey.toString()).getCurNodeIDStart())){
                logger.error("keys dont match error ");
                throw (Throwable) ExceptionInInitializerError;
            }

            int endIndex = (i+1)%hashRingSize;
            int preIndex = (i==0) ?hashRingSize-1: i-1;
            BigInteger hashEnd = keyArray.get(endIndex);
            String[] hashRange = createHashRange(curServerKey.toString(),hashEnd.toString());

            HashRing.get(curServerKey.toString()).setNextNodeID(hashEnd.toString());
            HashRing.get(curServerKey.toString()).setPreNodeID(keyArray.get(preIndex).toString());
            HashRing.get(curServerKey.toString()).setNodeHashRange(hashRange);
        }
    }

    public void addNewNodeByNode(ECSNode node) throws Throwable {
        
        BigInteger ECSNodeID = mdKey(node.getNodeName());
        int newElementIndex = -1;

        // printHashRing("addNewNodeByNode before");
        // System.out.println("==================================");
        // System.out.println("All nodes (before): " + keyArray);
        // System.out.println("Adding ECSNodeID: " + ECSNodeID);

        for (int i = 0; i < this.hashRingSize; i++){
            int compare = ECSNodeID.compareTo(keyArray.get(i));

            // ECSNodeID < keyarray node id, add before current node
            if (compare == -1){
                keyArray.add(i, ECSNodeID);
                newElementIndex = i;
                break;
            } 
            // ECSNodeID > last keyarray node id, add at last
            else if (compare == 1 && i == hashRingSize - 1){
                newElementIndex = hashRingSize;
                keyArray.add(hashRingSize, ECSNodeID);
                break;
            }
            // ECSNodeID > keyarray node id
            else if (compare == 1){
                continue;
            }
            // error: ECSNodeID = keyarray node id
            else if (compare == 0){
                logger.error("No two server nodes should have the same index");
                throw (Throwable) ExceptionInInitializerError;
            } 
            else {
                logger.error("I should not be here, something went wrong (ECSConsistantHashRing.java addNewNodeByNode)");
            }
        }

        // System.out.println("newElementIndex: " + newElementIndex);
        // System.out.println("Adding ECSNodeID: " + ECSNodeID);
        // System.out.println("All nodes (after): " + keyArray);
        // System.out.println("==================================");

        this.hashRingSize += 1;
        int nextIndex = (newElementIndex+1)%hashRingSize;
        int preIndex = (newElementIndex==0) ?hashRingSize-1: newElementIndex-1;
        String[] hashRange = createHashRange(ECSNodeID.toString(),keyArray.get(nextIndex).toString());

        node.setNextNodeID(keyArray.get(nextIndex).toString());
        node.setNodeHashRange(hashRange);
        node.setPreNodeID(keyArray.get(preIndex).toString());

        HashRing.put(ECSNodeID.toString(),node);
        // update previous node of the newly added node in hashring
        HashRing.get(keyArray.get(preIndex).toString()).updateNodeDataBehind(ECSNodeID.toString());
        //update the node after the newly added node in hashring
        HashRing.get(keyArray.get(nextIndex).toString()).updateNodeDataBefore(ECSNodeID.toString());

        // printHashRing("addNewNodeByNode after");
    }

    public boolean addNewNodeByNodes(Collection<ECSNode> nodes){
        for (ECSNode i : nodes){
            try {
                addNewNodeByNode(i);
            } catch (Throwable throwable) {
                throwable.printStackTrace();
                return false;
            }
        }
        return true;
    }

    private String[] createHashRange(String s, String e){
        String[] hashRange = new String[2];
        hashRange[0] = s;
        hashRange[1] = e;
        return hashRange;
    }

    public void removeNodebyNodeID(BigInteger nodeID) throws Throwable {
        
        int index = keyArray.indexOf(nodeID);
        if (index == -1){
            logger.error("List does not contain this element");
            throw (Throwable) RuntimeException;
        }

        // printHashRing("removeNodebyNodeID before");
        // System.out.println("==================================");
        // System.out.println("All nodes (before): " + keyArray);
        // System.out.println("Removing ECSNodeID: " + nodeID);

        int nextIndex = (index + 1) % hashRingSize;
        int preIndex = (index == 0) ? hashRingSize - 1 : index - 1;
        HashRing.get(keyArray.get(nextIndex).toString()).updateNodeDataBefore(keyArray.get(preIndex).toString());;
        HashRing.get(keyArray.get(preIndex).toString()).updateNodeDataBehind(keyArray.get(nextIndex).toString());
        keyArray.remove(index);

        // System.out.println("newElementIndex: " + index);
        // System.out.println("Adding ECSNodeID: " + nodeID);
        // System.out.println("All nodes (after): " + keyArray);
        // System.out.println("==================================");

        hashRingSize -=1;
        IECSNode delNode = HashRing.remove(nodeID.toString());

        if (delNode == null){
            logger.error("Something is very wrong");
        }

        // printHashRing("removeNodebyNodeID after");

    }

    public void removeNodebyServerName(String serverName) throws Throwable {
        BigInteger ECSNodeID = mdKey(serverName);
        removeNodebyNodeID(ECSNodeID);
    }

    public HashMap<String, IECSNode> getHashRing(){
        return HashRing;
    }

    public HashMap<String, Metadata> getMetadata(){
        HashMap<String, Metadata> metadataMap = new HashMap<>();
        for (String server : HashRing.keySet()){
            IECSNode node = HashRing.get(server);
            BigInteger prev = new BigInteger(node.getPreNodeID());
            BigInteger start = new BigInteger(node.getNodeHashRange()[0]);
            BigInteger stop = new BigInteger(node.getNodeHashRange()[1]);
            Metadata metadata = new Metadata(node.getNodeHost(), node.getNodePort(), prev, start, stop);
            metadataMap.put(server, metadata);
        }
        return metadataMap;
    }

    /**
     * helper function for getting MD5 hash key
     * may need to move to some shared class for being visible for both client and server
     */
    public BigInteger mdKey (String key) throws NoSuchAlgorithmException {
        MessageDigest md = MessageDigest.getInstance("MD5");
        byte[] md_key = md.digest(key.getBytes());
        return new BigInteger(1, md_key);
    }

    public ECSNode getECSNodeFromName(String serverName) throws NoSuchAlgorithmException {
        String [] addr_port = serverName.split(":");
        String addr = addr_port[0];
        String port = addr_port[1];

        BigInteger hashStart = mdKey(serverName);
        ECSNode newNode = new ECSNode(serverName,addr,Integer.parseInt(port),hashStart.toString());

        return newNode;
    }

    /**
     * Print consistent hash ring that includes all alive KVServer. 
     * @param message Message print associated with hash ring information
     */
    public void printHashRing(String message){
        
        System.out.println("------------ " + message + " ----------------");
        Iterator<Map.Entry<String,IECSNode>> it = HashRing.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String,IECSNode> pair = it.next();
            ECSNode node = (ECSNode) pair.getValue();
            System.out.println("Node Name: " + node.getNodeName());
            System.out.println("Node ID: " + node.getCurNodeIDStart());
            System.out.println("Start: " + node.getCurNodeIDStart());
            System.out.println("Stop: " + node.getNextNodeID());
            System.out.println("------------------");
        }
        System.out.println("Key Array: " + keyArray);
        System.out.println("----------------------------------------------");
        
    }

}
