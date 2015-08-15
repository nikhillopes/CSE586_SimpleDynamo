package edu.buffalo.cse.cse486586.simpledynamo;

import android.util.Log;

import java.io.Serializable;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;
import java.util.HashMap;

/**
 * Created by nikhil on 4/11/15.
 */
public class SimpleDynamoMessage implements Serializable {
    private int messageType;
    private int destinationNode;
    private String key;
    private String value;
    private String selection;
    private HashMap<String, String> queryResult;

    private String genHash(String input) {
        try {
            MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
            byte[] sha1Hash = sha1.digest(input.getBytes());
            Formatter formatter = new Formatter();
            for (byte b : sha1Hash) {
                formatter.format("%02x", b);
            }
            return formatter.toString();
        } catch (NoSuchAlgorithmException e) {
            Log.e("SimpleDynamoActivity", "genHash Failed");
        }
        return null;
    }

    public SimpleDynamoMessage(MessageType messageType) {
        this.messageType = messageType.getIntVal(messageType);
    }

    public MessageType getMessageType() {
        return MessageType.getMessageType(this.messageType);
    }

    public DynamoNode getDestinationNode() {
        return new DynamoNode(destinationNode, genHash(Integer.toString(destinationNode/2)));
    }

    public void setDestinationNode(DynamoNode destinationNode) {
        this.destinationNode = destinationNode.getPort();
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public String getSelection() {
        return selection;
    }

    public void setSelection(String selection) {
        this.selection = selection;
    }

    public HashMap<String, String> getQueryResult() {
        return queryResult;
    }

    public void setQueryResult(HashMap<String, String> queryResult) {
        this.queryResult = queryResult;
    }

    @Override
    public String toString() {
        return "Msg{" +
                "mT=" + MessageType.getMessageType(this.messageType).toString() +
                ", desN=" + destinationNode/2 +
                ", key='" + key + '\'' +
                ", val='" + value + '\'' +
                ", sel='" + selection + '\'' +
                ", qRe=" + queryResult +
                '}';
    }
}
