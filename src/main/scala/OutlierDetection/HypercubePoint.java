package OutlierDetection;

import java.io.Serializable;

public class HypercubePoint implements Serializable {

    public double[] coords;
    public long arrival;
    public int hypercubeID;
    public int partitionID;

    public HypercubePoint(){

    }

    public HypercubePoint(double[] vals, long timeOfArrival){
        this.coords = vals;
        this.arrival = timeOfArrival;
    }

    public HypercubePoint(double[] vals, long timeOfArrival, int hID, int pID){
        this.coords = vals;
        this.arrival = timeOfArrival;
        this.hypercubeID = hID;
        this.partitionID = pID;
    }

    public double[] getCoords() {
        return coords;
    }

    public void setCoords(double[] coords) {
        this.coords = coords;
    }

    public long getArrival() {
        return arrival;
    }

    public void setArrival(long arrival) {
        this.arrival = arrival;
    }

    public int getHypercubeID() {
        return hypercubeID;
    }

    public void setHypercubeID(int hypercubeID) {
        this.hypercubeID = hypercubeID;
    }

    public int getPartitionID() {
        return partitionID;
    }

    public void setPartitionID(int partitionID) {
        this.partitionID = partitionID;
    }


}
