package ecs;

public class ECSNode implements IECSNode{


    private String nodeName;
    private String nodeHost;
    private int nodePort;
    private String[] hashRange;

    public ECSNode(String nodeName,String nodeHost, int nodePort, String[] hashRange){

        nodeName = nodeName;
        nodeHost = nodeHost;
        nodePort = nodePort;
        hashRange = hashRange;
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
