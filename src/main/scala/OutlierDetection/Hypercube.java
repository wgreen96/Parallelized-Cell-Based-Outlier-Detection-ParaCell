package OutlierDetection;

import akka.remote.WireFormats;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;

public class Hypercube implements Serializable {

    public double[] coords;
    public long arrival;
    public String hypercubeID;
    public int partitionID;
    public int hypercubeCount;
    public ArrayList<Double> centerOfCellCoords;

    public Hypercube(){

    }

    public Hypercube(double[] vals, long timeOfArrival){
        this.coords = vals;
        this.arrival = timeOfArrival;
    }

    public Hypercube(double[] vals, long timeOfArrival, String hID, int pID, ArrayList<Double> means){
        this.coords = vals;
        this.arrival = timeOfArrival;
        this.hypercubeID = hID;
        this.partitionID = pID;
        this.centerOfCellCoords = means;
    }

    public Hypercube(double[] vals, long timeOfArrival, String hID, int pID, ArrayList<Double> means, int hypcubeCnt){
        this.coords = vals;
        this.arrival = timeOfArrival;
        this.hypercubeID = hID;
        this.partitionID = pID;
        this.centerOfCellCoords = means;
        this.hypercubeCount = hypcubeCnt;
    }

    public Hypercube(long timeOfArrival, String hID, int hycubeCnt, int pID){
        this.arrival = timeOfArrival;
        this.hypercubeID = hID;
        this.hypercubeCount = hycubeCnt;
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

    public String getHypercubeID() {
        return hypercubeID;
    }

    public void setHypercubeID(String hypercubeID) {
        this.hypercubeID = hypercubeID;
    }

    public int getPartitionID() {
        return partitionID;
    }

    public void setPartitionID(int partitionID) {
        this.partitionID = partitionID;
    }

    public int getHypercubeCount() {
        return hypercubeCount;
    }

    public void setHypercubeCount(int hypercubeCount) {
        this.hypercubeCount = hypercubeCount;
    }

    public ArrayList<Double> getCenterOfCellCoords() {
        return centerOfCellCoords;
    }

    public void setCenterOfCellCoords(ArrayList<Double> centerOfCellCoords) {
        this.centerOfCellCoords = centerOfCellCoords;
    }

    public static int getKey(Hypercube point){
        return point.partitionID;
    }

    public String coordsToString(){
        return Arrays.toString(this.coords);
    }

    public String meansToString(){
        return this.centerOfCellCoords.toString();
    }

//    public String toString() {
//        return "Coords: " + this.coordsToString() +
//                ", Arrival: " + this.arrival +
//                ", HypercubeID: " + this.hypercubeID +
//                ", HyperoctantID: " + this.hyperoctantID +
//                ", PartitionID: " + this.partitionID +
//                ", HypercubeCount: " + this.hypercubeCount +
//                ", Mean coords: " + this.meansToString();
//    }

//    public String toString() {
//        return "Coords: " + this.coordsToString() +
//                ", Arrival: " + this.arrival +
//                ", HypercubeID: " + this.hypercubeID +
//                ", HypercubeCount: " + this.hypercubeCount;
//    }

    public String toString() {
        return this.coordsToString();
    }

}
