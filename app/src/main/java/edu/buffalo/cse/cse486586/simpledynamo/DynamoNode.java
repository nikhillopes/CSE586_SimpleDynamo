package edu.buffalo.cse.cse486586.simpledynamo;

import java.net.Socket;
import java.util.Comparator;

/**
 * Created by nikhil on 4/11/15.
 */
public class DynamoNode {
    private int port;
    private String ID;

    public DynamoNode(int port, String ID) {
        this.port = port;
        this.ID = ID;
    }

    public int getPort() {
        return port;
    }

    public String getID() {
        return ID;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DynamoNode that = (DynamoNode) o;

        if (port != that.port) return false;
        if (!ID.equals(that.ID)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = port;
        result = 31 * result + ID.hashCode();
        return result;
    }

    public static class NodeComparator implements Comparator<DynamoNode> {
        @Override
        public int compare(DynamoNode lhs, DynamoNode rhs) {
            if(lhs==null){
                return 1;
            } else if(rhs==null){
                return -1;
            }
            return lhs.getID().compareTo(rhs.getID());
        }
    }

    @Override
    public String toString() {
        return "{" +
                "port=" + port/2 +
                //", ID='" + ID + '\'' +
                '}';
    }
}
