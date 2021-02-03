package OutlierDetection;

import be.tarsos.lsh.families.DistanceMeasure;
import be.tarsos.lsh.families.HashFamily;
import be.tarsos.lsh.families.HashFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import be.tarsos.lsh.*;

import java.text.DecimalFormat;
import java.util.*;
import java.util.Vector;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class OutlierDetectionTheThird extends ProcessFunction<Hypercube, String> {


    static double radius;
    //k neigbors required to be nonoutliers
    static int kNeighs;
    //The amount of time after processing that a data point can live. Is measured in milliseconds
    static long lifeThreshold;
    static double dimensions;

    Map<Double, Integer> hypercubeState = new HashMap<Double, Integer>();
    Map<Tuple2, Integer> hyperOctantState = new HashMap<Tuple2, Integer>();
    Map<Double, LinkedList> timeState = new HashMap<Double, LinkedList>();
    Map<Double, double[]> meanCoords = new HashMap<Double, double[]>();
    Map<Double, ArrayList> setOfDataPoints = new HashMap<Double, ArrayList>();
    ArrayList<Double> sortedIDs = new ArrayList<Double>();

    @Override
    public void processElement(
            Hypercube currPoint,
            Context context,
            Collector<String> collector) throws Exception {

        //Key data points by HypercubeID for easier extraction later
        if(setOfDataPoints.get(currPoint).isEmpty()){
            ArrayList<double[]> newList = new ArrayList<>();
            newList.add(currPoint.meanMultis);
            setOfDataPoints.put(currPoint.hypercubeID,newList);
        }else{
            ArrayList<double[]> newList = setOfDataPoints.get(currPoint.hypercubeID);
            newList.add(currPoint.meanMultis);
            setOfDataPoints.put(currPoint.hypercubeID,newList);
        }

        boolean newHypercube = false;
        //Update hypercubeState with newest information
        double currHypID = currPoint.hypercubeID;
        double currHypOctID = currPoint.hyperoctantID;
        int currHypCount = currPoint.hypercubeCount;
        if(!hypercubeState.containsKey(currHypID)){
            newHypercube = true;
        }
        hypercubeState.put(currHypID, currHypCount);

        //Create a sorted list of hypercubes for searching in Outlier Detection
        if(newHypercube == true){
            //Add mean values for hypercube mean state
            meanCoords.put(currHypID, currPoint.meanMultis);
            //Insert hypercubeID in array while keeping a sorted structure
            sortedIDs.add(currHypID);
            Collections.sort(sortedIDs);
        }

        //Update Hyperoctant state
        Tuple2<Double, Double> hyOctantID = new Tuple2<>(currHypID, currHypOctID);
        if(hyperOctantState.containsKey(hyOctantID)){
            int newVal = hyperOctantState.get(hyOctantID) + 1;
            hyperOctantState.put(hyOctantID, newVal);
        }else{
            hyperOctantState.put(hyOctantID, 1);
        }

        //Update time state
        LinkedList hypercubeQueue;
        //If state already exist, return current time state
        if(timeState.containsKey(currHypID)){
            hypercubeQueue = timeState.get(currHypID);
        }else{
            hypercubeQueue = new LinkedList<>();
        }
        //Add newest time to time state. Using arrival is not perfect, there will be some deviations within 100ms range in the FIFO queue because of parallel processing
        hypercubeQueue.add(currPoint.arrival);

        //Start working on Outlier Detection
        long currentTime = context.timerService().currentProcessingTime();
        //Check if data points in FIFO queue are to be pruned
        boolean pruning = true;
        while(pruning){
            //Check if head of Queue - currentTime is greater than threshold.
            if(hypercubeQueue.peek() != null && (currentTime - (long) hypercubeQueue.peek()) > lifeThreshold){
                //If so: remove the head, run outlier detection
                hypercubeQueue.remove();
                //Start Outlier Detection by checking if current cell is less than k
                if(hypercubeState.get(currHypID) < kNeighs){

                    //Need to return sum of level 1 neighbors. Start by parsing hypercubeID
                    int totalNeighborhoodCount = hypercubeState.get(currHypID);
                    ArrayList<Double> setOfNeighs = new ArrayList<>();

                    double[] setOfMulties = meanCoords.get(currHypID);

                    //Compare ID of current Hypercube to the rest of hypercubes
                    for(Double currCubes: sortedIDs){
                        //Skip comparison to self
                        if(currCubes == currHypID){
                            continue;
                        }
                        double[] setOfMulties2 = meanCoords.get(currCubes);

                        double distance = 0;
                        //Calculate distance function
                        for(int currIndex = 0; currIndex < setOfMulties.length; currIndex++){
                            distance += Math.pow(setOfMulties[currIndex] - setOfMulties2[currIndex], 2);
                        }
                        distance = Math.sqrt(distance);
                        double upperBound = distance + (radius/2);
                        double lowerBound = distance - (radius/2);

                        //If value + and - (diagonal/2) is less than radius, level 1
                        if(upperBound < radius && lowerBound < radius){
                            setOfNeighs.add(currCubes);
                            int thisCellCount = hypercubeState.get(currCubes);
                            totalNeighborhoodCount += thisCellCount;
                            if(totalNeighborhoodCount >= kNeighs){
                                break;
                            }
                        }
                        //If one value is less than radius and the other is greater, level 2
                        else if((upperBound > radius && lowerBound < radius) || (upperBound < radius && lowerBound > radius)){
                            setOfNeighs.add(currCubes);
                        }
                        //If both are greater, cell is out of neighborhood


                    }

                    //Now we have compared level 1 neighbors. If that still isn't above k, need to get level 2 neighbors
                    if(totalNeighborhoodCount < kNeighs){
                        //Start off by getting all data points from level 1 and 2 cells
                        ArrayList<double[]> setOfNeighPoints = new ArrayList<>();
                        Vector<double[]> test = new Vector<>();
                        for(Double currNeighs : setOfNeighs){
                            setOfNeighPoints.addAll(setOfDataPoints.get(currNeighs));
                            test.addAll(setOfDataPoints.get(currNeighs));
                        }
                        //Pass data to LSH
                        //LSH approxDetection = new LSH(setOfNeighPoints, HashFamily.createHashFamily());
                        //approxDetection.query(test, kNeighs);
                    }
                    //System.out.println(totalNeighborhoodCount);

                }



            }else{
                //If not, stop checking. No further elements can be removed because any element after the head is unlikely to be newer
                pruning = false;
            }
            //TODO Prune data point from setOfDataPoints
        }
        //After doing outlier detection
        timeState.put(currHypID, hypercubeQueue);



    }
}
