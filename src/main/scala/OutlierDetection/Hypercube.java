package OutlierDetection;
import java.io.Serializable;

public class Hypercube implements Serializable {
    //TODO I want to have seperate storage for hypercubes based off the partition they belong to
    //TODO I want to create a function that returns the hypercube's ID based off its coordinates values
    //Specifically, I think creating a hypercube and getting its coordinates from there is faster than searching an array of unsorted arrays

    public float[] coordValues;
    public int hypercubeID;
    public int partitionKey;

    public Hypercube(){

    }

}
