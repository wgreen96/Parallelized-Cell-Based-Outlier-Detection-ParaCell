package OutlierDetection;
import java.io.Serializable;
import java.util.ArrayList;

public class Hypercube implements Serializable {
    //TODO I want to have seperate storage for hypercubes based off the partition they belong to
    //Specifically, I think creating a hypercube and getting its coordinates from there is faster than searching an array of unsorted arrays

    //I don't actually think this is needed. At least not in a way that is clear at the moment
    public float[] coordValues;
    public int hypercubeID;
    public int partitionKey;

    public Hypercube(float[] coords){
        this.coordValues = coords;
    }

    public void setHypercubeID(int hypercubeID) {
        this.hypercubeID = hypercubeID;
    }

    public int getHypercubeID() {
        return hypercubeID;
    }

    public void setPartitionKey(int partitionKey) {
        this.partitionKey = partitionKey;
    }

    public int getPartitionKey() {
        return partitionKey;
    }

}
