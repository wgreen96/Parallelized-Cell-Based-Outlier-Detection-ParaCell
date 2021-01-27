package OutlierDetection;

import java.io.Serializable;

public class HypercubePoint implements Serializable {

    public double[] coords;
    public long arrival;
    public double hypercubeID;
    public int partitionID;

    public HypercubePoint(){

    }

    public HypercubePoint(double[] vals, long timeOfArrival){
        this.coords = vals;
        this.arrival = timeOfArrival;
    }

    public HypercubePoint(double[] vals, long timeOfArrival, double hID, int pID){
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
        System.out.println(this.arrival);
        return arrival;
    }

    public void setArrival(long arrival) {
        this.arrival = arrival;
    }

    public double getHypercubeID() {
        return hypercubeID;
    }

    public void setHypercubeID(double hypercubeID) {
        this.hypercubeID = hypercubeID;
    }

    public int getPartitionID() {
        return partitionID;
    }

    public void setPartitionID(int partitionID) {
        this.partitionID = partitionID;
    }

    public static int getKey(HypercubePoint point){
        return point.partitionID;
    }

    public String coordsToString(){
        String coordString = "";
        for(double coorVals : this.coords){
            coordString += Double.toString(coorVals) + ", ";
        }
        return coordString;
    }

    public String toString() {
        return "Coords: '" + this.coordsToString() + "', Arrival: '" + this.arrival + "', HypercubeID: '" + this.hypercubeID + "'" + "', PartitionID: '" + this.partitionID + "'";
    }

}
