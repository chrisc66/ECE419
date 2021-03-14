package ecs;

import org.apache.log4j.Logger;

import shared.messages.Metadata;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

/**This class contains a Hash Ring which stores the **/
public class ECSConsistantHashRing {


    /**The server names are defined as Address+port**/
    private static Logger logger = Logger.getRootLogger();
    private HashMap<String,IECSNode> HashRing = new HashMap<>(); //key: BigInteger in string format
    private List<BigInteger> keyArray= new ArrayList<>();
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
        for(int i=0; i < hashRingSize; i++){
            /** Assume server name are in the format of ip:port **/
            String serverName = ServerNames.get(i);

            ECSNode newNode = getECSNodeFromName(serverName);
            HashRing.put(newNode.getCurNodeIDStart(),newNode);
            keyArray.add(new BigInteger(newNode.getCurNodeIDStart()));
        }
        Collections.sort(keyArray);

        for(int i=0; i < hashRingSize; i++){
            BigInteger curServerKey = keyArray.get(i);
            if(!curServerKey.toString().equals(HashRing.get(curServerKey.toString()).getCurNodeIDStart())){
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
        BigInteger ECSNodeID = NametoHashConverter(node.getNodeName());
        int newElementIndex = -1;

        for (int i=0; i < this.hashRingSize; i++){
            int compare = ECSNodeID.compareTo(keyArray.get(i));
            if (compare == 0){
                //same as the input integer
                logger.error("No two server node should have the same index");
                throw (Throwable) ExceptionInInitializerError;
            }else if (compare == -1){
                keyArray.add(i,ECSNodeID);
                newElementIndex = i;
                break;
            }else if(compare == 1){
                //ecsnodeid > keyarray node id
                newElementIndex = hashRingSize;
                keyArray.add(hashRingSize,ECSNodeID);
                break;
            }
        }

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

    }

    /**Assume serverName format is ip:port**/
    public void addNewNode(String serverName) throws Throwable {
        String [] addr_port = serverName.split(":");
        String addr = addr_port[0];
        String port = addr_port[1];

        BigInteger ECSNodeID = NametoHashConverter(serverName);
        int newElementIndex = -1;

        for (int i=0; i < hashRingSize; i++){
            int compare = ECSNodeID.compareTo(keyArray.get(i));
            if (compare == 0){
                //same as the input integer
                logger.error("No two server node should have the same index");
                throw (Throwable) ExceptionInInitializerError;
            }else if (compare == -1){
                keyArray.add(i,ECSNodeID);
                newElementIndex = i;
                break;
            }else if(compare == 1){
                //ecsnodeid > keyarray node id
            }
        }

        hashRingSize+=1;
        int nextIndex = (newElementIndex+1)%hashRingSize;
        int preIndex = (newElementIndex==0) ?hashRingSize-1: newElementIndex-1;
        String[] hashRange = createHashRange(ECSNodeID.toString(),keyArray.get(nextIndex).toString());
        ECSNode newNode = new ECSNode(serverName,addr,Integer.parseInt(port),
                ECSNodeID.toString(),hashRange,keyArray.get(preIndex).toString(),keyArray.get(nextIndex).toString());
        HashRing.put(ECSNodeID.toString(),newNode);
        // update previous node of the newly added node in hashring
        HashRing.get(keyArray.get(preIndex).toString()).updateNodeDataBehind(ECSNodeID.toString());
        //update the node after the newly added node in hashring
        HashRing.get(keyArray.get(nextIndex).toString()).updateNodeDataBefore(ECSNodeID.toString());

    }

    public boolean addNewNodeByNodes(Collection<ECSNode> nodes){

        for(ECSNode i : nodes){
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
        int nextIndex = (index+1)%hashRingSize;
        int preIndex = (index==0) ?hashRingSize-1: index-1;
        HashRing.get(keyArray.get(nextIndex).toString()).setPreNodeID(keyArray.get(preIndex).toString());
        HashRing.get(keyArray.get(preIndex).toString()).setNextNodeID(keyArray.get(nextIndex).toString());
        keyArray.remove(index);

        hashRingSize -=1;
        IECSNode delNode = HashRing.remove(nodeID.toString());

        if (delNode == null){
            logger.error("Something is very wrong");
        }

    }

    public void removeNodebyServerName(String serverName) throws Throwable {
        BigInteger ECSNodeID = NametoHashConverter(serverName);
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
            // HERE!!!!!
            BigInteger start = new BigInteger(node.getNodeHashRange()[0]);
            BigInteger stop = new BigInteger(node.getNodeHashRange()[1]);
            Metadata metadata = new Metadata(node.getNodeHost(), node.getNodePort(), prev, start, stop);
            metadataMap.put(server, metadata);
        }
        return metadataMap;
    }

    public BigInteger NametoHashConverter(String key) throws NoSuchAlgorithmException {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] md_key = md.digest(key.getBytes());
            BigInteger md_key_bi = new BigInteger(1, md_key);
            return md_key_bi;
        } catch (NoSuchAlgorithmException e) {
            logger.error("NoSuchAlgorithmException occured!");
            throw new NoSuchAlgorithmException();
        }

    }

    public ECSNode getECSNodeFromName(String serverName) throws NoSuchAlgorithmException {
        String [] addr_port = serverName.split(":");
        String addr = addr_port[0];
        String port = addr_port[1];

        BigInteger hashStart = NametoHashConverter(serverName);
        ECSNode newNode = new ECSNode(serverName,addr,Integer.parseInt(port),hashStart.toString());

        return  newNode;
    }

}
