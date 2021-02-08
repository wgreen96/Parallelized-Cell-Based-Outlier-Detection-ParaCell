package OutlierDetection;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import smile.neighbor.MPLSH;

import java.util.*;

public class OutlierDetectionTheFourth extends ProcessAllWindowFunction<Hypercube, String, TimeWindow> {


    Map<Double, Tuple2> hypercubeState = new HashMap<Double, Tuple2>();
    Map<Tuple2, Integer> hyperOctantState = new HashMap<Tuple2, Integer>();
    Map<Double, Long> lastModification = new HashMap<>();
    Map<Double, ArrayList> setOfDataPoints = new HashMap<Double, ArrayList>();
    ArrayList<Hypercube> dataToBePruned = new ArrayList<>();
    ArrayList<Double> sortedIDs = new ArrayList<Double>();

    static long windowSize;
    static long slideSize;
    static int kNeighs;
    static int dimensions;
    static double radius;




    @Override
    public void process(Context context,
                        Iterable<Hypercube> windowPoints,
                        Collector<String> collector) throws Exception {


        //Get window time
        long windowEndTime = context.window().getEnd();

        //Start off by iterating through the current window
        for(Hypercube currPoints: windowPoints){

            double currHypID = currPoints.hypercubeID;
            double currHypOctID = currPoints.hyperoctantID;
            long currTime = currPoints.arrival;
            int currHypCount = currPoints.hypercubeCount;
            double[] currHypMeanCoords = currPoints.centerOfCellCoords;

            //Check if the state is new
            if(!hypercubeState.containsKey(currHypID)){
                //If new, create entry for HypercubeState and lastModified
                Tuple2<Integer, double[]> hypercubeStateValue = new Tuple2<>(currHypCount, currHypMeanCoords);
                hypercubeState.put(currHypID, hypercubeStateValue);
                lastModification.put(currHypID, currTime);
                //Created a sorted list of HypercubeIDs for faster Outlier Detection
                sortedIDs.add(currHypID);
                Collections.sort(sortedIDs);
            }else{
                //Do error checking for out of order data points by only updating state if data point is newer
                if(currTime > lastModification.get(currHypID)){
                    Tuple2<Integer, double[]> hypercubeStateValue = new Tuple2<>(currHypCount, currHypMeanCoords);
                    hypercubeState.put(currHypID, hypercubeStateValue);
                    lastModification.put(currHypID, currTime);
                }
            }

            //Update Hyperoctant state
            Tuple2<Double, Double> hyOctantID = new Tuple2<>(currHypID, currHypOctID);
            if(hyperOctantState.containsKey(hyOctantID)){
                int newVal = hyperOctantState.get(hyOctantID) + 1;
                hyperOctantState.put(hyOctantID, newVal);
            }else{
                hyperOctantState.put(hyOctantID, 1);
            }

            //Key data points by HypercubeID for easier extraction later
            ArrayList<double[]> newList;
            if(!setOfDataPoints.containsKey(currPoints)){
                newList = new ArrayList<>();
            }else{
                newList = setOfDataPoints.get(currPoints.hypercubeID);
            }
            newList.add(currPoints.centerOfCellCoords);
            setOfDataPoints.put(currPoints.hypercubeID,newList);

            //Finally, check if data points will be pruned before the next slide
            if((currPoints.arrival + windowSize) < windowEndTime){
                dataToBePruned.add(currPoints);
            }

        }

        //Run Outlier Detection on data about to be pruned
        for(Hypercube prunedData : dataToBePruned){

            double currHypID = prunedData.hypercubeID;
            Tuple2<Integer, double[]> hypStateValue = hypercubeState.get(currHypID);
            int hypStateCount = hypStateValue.f0;
            double[] centerCoords = hypStateValue.f1;

            //Start Outlier Detection by checking if current cell is less than k
            if(hypStateCount < kNeighs){

                //Need to return sum of level 1 neighbors. Start by parsing hypercubeID
                int totalNeighborhoodCount = hypStateCount;
                ArrayList<Double> setOfNeighs = new ArrayList<>();

                //Compare ID of current Hypercube to the rest of hypercubes
                for(Double currCubes: sortedIDs) {
                    //Skip comparison to self
                    if (currCubes == currHypID) {
                        continue;
                    }

                    Tuple2<Integer, double[]> hypStateValue2 = hypercubeState.get(currCubes);
                    double[] centerCoords2 = hypStateValue2.f1;

                    double distance = 0;
                    //Calculate distance function
                    for(int currIndex = 0; currIndex < centerCoords.length; currIndex++){
                        distance += Math.pow(centerCoords[currIndex] - centerCoords2[currIndex], 2);
                    }
                    distance = Math.sqrt(distance);
                    double upperBound = distance + (radius/2);
                    double lowerBound = distance - (radius/2);

                    //If value + and - (diagonal/2) is less than radius, level 1
                    if(upperBound < radius && lowerBound < radius){
                        setOfNeighs.add(currCubes);
                        int thisCellCount = hypStateValue2.f0;
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

                    System.out.println("CurrentID: " + currHypID);
                    System.out.println("Compared ID: " + currCubes);
                    System.out.println("Difference in 1st place: " + (centerCoords[0] - centerCoords2[0]));
                    System.out.println("DIfference: " + distance);

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
                    //Pass query (current data point) and neighbors to LSH
                    double hashFunctions = Math.log(setOfNeighPoints.size());
                    int LValue = 0;
                    if(hashFunctions % 1 >= 0.5){
                        LValue = (int) Math.ceil(hashFunctions);
                    }else{
                        LValue = (int) Math.floor(hashFunctions);
                    }
                    MPLSH LSH = new MPLSH(dimensions, LValue, 3, radius);


                }

            }
        }

        //Modify states to reflect environment after data points have been pruned
        setOfDataPoints.clear();
        dataToBePruned.clear();
        //Instead of running 2 for loops that do the same thing, have HyperOctantState be modifed at end of Outlier Loop
        //Or atleast store that information so it can easily be modified instead of running the loop again


    }



}
