package ecs;

public class ECSNode implements IECSNode{


    private String nodeName;
    private String nodeHost;
    private String preNodeID;
    private String curNodeIDStart;
    private String nextNodeID;
    private int nodePort;
    private String[] hashRange;

    public ECSNode(String nodeName,String nodeHost, int nodePort, String curNodeIDStart, String[] hashRange){

        nodeName = nodeName;
        nodeHost = nodeHost;
        nodePort = nodePort;
        hashRange = hashRange;
        curNodeIDStart = curNodeIDStart;

        preNodeID = null;
        nextNodeID = null;
    }

    public ECSNode(String nodeName,String nodeHost, int nodePort, String curNodeIDStart){

        nodeName = nodeName;
        nodeHost = nodeHost;
        nodePort = nodePort;
        curNodeIDStart = curNodeIDStart;
        hashRange = null;

        preNodeID = null;
        nextNodeID = null;
    }

    public ECSNode(String nodeName,String nodeHost, int nodePort,String curNodeIDStart,
                   String[] hashRange, String preNodeID, String nextNodeID){

        nodeName = nodeName;
        nodeHost = nodeHost;
        nodePort = nodePort;
        hashRange = hashRange;
        curNodeIDStart = curNodeIDStart;

        preNodeID = preNodeID;
        nextNodeID = nextNodeID;
    }

    public void updateNodeDataBehind(String endRange){
        hashRange[1]= endRange;
        nextNodeID = endRange;
    }

    public void updateNodeDataBefore(String prevRange){
        preNodeID = prevRange;
    }


    public void setPreNodeID(String preNodeID) {
        this.preNodeID = preNodeID;
    }

    public void setNextNodeID(String nextNodeID) {
        this.nextNodeID = nextNodeID;
    }

    public String getCurNodeIDStart() {
        return curNodeIDStart;
    }

    public String getNextNodeID() {
        return nextNodeID;
    }

    public String getPreNodeID() {
        return preNodeID;
    }


    @Override
    public String getNodeName() {
        return nodeName;
    }

    @Override
    public String getNodeHost() {
        return nodeHost;
    }

    @Override
    public int getNodePort() {
        return nodePort;
    }

    @Override
    public String[] getNodeHashRange() {
        return hashRange;
    }

    public void setNodeHashRange(String[] hashRange){
        hashRange = hashRange;
    }
}
