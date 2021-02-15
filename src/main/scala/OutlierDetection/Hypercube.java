package OutlierDetection;

import java.io.Serializable;
import java.util.Comparator;

public class Hypercube implements Serializable {

    public double[] coords;
    public long arrival;
    public double hypercubeID;
    public double hyperoctantID;
    public int partitionID;
    public int hypercubeCount;
    public double[] centerOfCellCoords;

    public Hypercube(){

    }

    public Hypercube(double[] vals, long timeOfArrival){
        this.coords = vals;
        this.arrival = timeOfArrival;
    }

    public Hypercube(double[] vals, long timeOfArrival, double hID, double octID, int pID, double[] means){
        this.coords = vals;
        this.arrival = timeOfArrival;
        this.hypercubeID = hID;
        this.hyperoctantID = octID;
        this.partitionID = pID;
        this.centerOfCellCoords = means;
    }

    public Hypercube(double[] vals, long timeOfArrival, double hID, double octID, int pID, double[] means, int hypcubeCnt){
        this.coords = vals;
        this.arrival = timeOfArrival;
        this.hypercubeID = hID;
        this.hyperoctantID = octID;
        this.partitionID = pID;
        this.centerOfCellCoords = means;
        this.hypercubeCount = hypcubeCnt;
    }

    public double[] getCoords() {
        return coords;
    }

    public void setCoords(double[] coords) {
        this.coords = coords;
    }

    public long getArrival() {
//        System.out.println(this.arrival);
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

    public double getHyperoctantID() {
        return hyperoctantID;
    }

    public void setHyperoctantID(double hyperoctantID) {
        this.hyperoctantID = hyperoctantID;
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

    public double[] getCenterOfCellCoords() {
        return centerOfCellCoords;
    }

    public void setCenterOfCellCoords(double[] centerOfCellCoords) {
        this.centerOfCellCoords = centerOfCellCoords;
    }

    public static int getKey(Hypercube point){
        return point.partitionID;
    }

    public String coordsToString(){
        String coordString = "";
        for(double coorVals : this.coords){
            coordString += Double.toString(coorVals) + ",";
        }
        return coordString;
    }

    public String meansToString(){
        String coordString = "";
        for(double coorVals : this.centerOfCellCoords){
            coordString += Double.toString(coorVals) + ", ";
        }
        return coordString;
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

    static class CustomerSortingComparator implements Comparator<Hypercube> {
        @Override
        public int compare(Hypercube cell1, Hypercube cell2) {
            int result = 0;
            // for comparison
            int x1 = Double.compare(cell1.coords[0],cell2.coords[0]);
            int x2 = Double.compare(cell1.coords[1],cell2.coords[1]);
            int x3 = Double.compare(cell1.coords[2],cell2.coords[2]);

            // 2-level comparison using if-else block
            if (x1 == 0) {
                if(x2 == 0){
                    result = x3;
                }else{
                    result = x2;
                }
            }else {
                result = x1;
            }
            return result;
        }
    }

}
