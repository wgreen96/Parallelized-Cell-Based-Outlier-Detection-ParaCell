package OutlierDetection;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import smile.neighbor.MPLSH;
import smile.neighbor.Neighbor;

import java.util.*;

public class OutlierDetectionTheFifth extends ProcessAllWindowFunction<Hypercube, Hypercube, TimeWindow> {


    Map<Double, Tuple2> hypercubeState = new HashMap<>();
    Map<Double, Long> lastModification = new HashMap<>();
    Map<Double, ArrayList> setOfDataPoints = new HashMap<>();
    ArrayList<Hypercube> potentialOutliers = new ArrayList<>();
    ArrayList<Double> uniqueKeys = new ArrayList<>();
    Map<Double, Tuple2> hypercubeNeighs = new HashMap<>();
    ArrayList<double[]> setOfMeanCoords = new ArrayList<>();

    static long slideSize;
    static int minPts;
    static int dimensions;
    static double radius;
    static double hypercubeSide;
    static int dimWithHighRange;
    int rangeOfVals = (int) Math.floor(radius/hypercubeSide);
    long cpuTime = 0L;
    double numberIterations = 0;

    @Override
    public void process(Context context,
                        Iterable<Hypercube> windowPoints,
                        Collector<Hypercube> collector) throws Exception {

        long time_init = System.currentTimeMillis();
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
                //If new, create entry for HypercubeState, lastModified, and setOfMeanCoords
                Tuple2<Integer, double[]> hypercubeStateValue = new Tuple2<>(currHypCount, currHypMeanCoords);
                hypercubeState.put(currHypID, hypercubeStateValue);
                lastModification.put(currHypID, currTime);
                setOfMeanCoords.add(currHypMeanCoords);
            }else{
                //Do error checking for out of order data points by only updating state if data point is newer
                if(currTime > lastModification.get(currHypID)){
                    Tuple2<Integer, double[]> hypercubeStateValue = new Tuple2<>(currHypCount, currHypMeanCoords);
                    hypercubeState.put(currHypID, hypercubeStateValue);
                    lastModification.put(currHypID, currTime);
                }
            }

            //Key data points by HypercubeID for easier extraction later
            ArrayList<double[]> newList;
            if(!setOfDataPoints.containsKey(currHypID)){
                newList = new ArrayList<>();

            }else{
                newList = setOfDataPoints.get(currHypID);
            }
            newList.add(currPoints.coords);
            setOfDataPoints.put(currHypID, newList);

            //Finally, collect the list of data points to be pruned
            if((currPoints.arrival + slideSize) > windowEndTime){
                potentialOutliers.add(currPoints);
            }

        }

        //Sort setOfMeanCoords by the dimension in dimWithHighRange
        Collections.sort(setOfMeanCoords, Comparator.comparingDouble(coord -> coord[dimWithHighRange - 1]));


        //Run outlier detection on the data points that will be pruned after this window is processed
        //This is going backwards because data points are being removed and doing so while going forwards breaks everything
        for(int pruneIndex = potentialOutliers.size() - 1; pruneIndex >= 0; pruneIndex--){

            Hypercube prunedData = potentialOutliers.get(pruneIndex);
            double currHypID = prunedData.hypercubeID;
            Tuple2<Integer, double[]> hypStateValue = hypercubeState.get(currHypID);
            int hypStateCount = hypStateValue.f0;
            double[] centerCoords = hypStateValue.f1;
            int level1NeighborhoodCount = 0;
            int totalNeighborhoodCount = 0;

            if(hypStateCount < minPts){

                //Binary search for meanCoords.dimWithHighRange within range currHyp.dimWithHighRange +- rangeOfVals
                int key = Collections.binarySearch(setOfMeanCoords, centerCoords, (cell1, cell2) -> {
                    if(cell1[dimWithHighRange-1] < (cell2[dimWithHighRange-1] - rangeOfVals)){
                        return -1;
                    }
                    else if(cell1[dimWithHighRange-1] > (cell2[dimWithHighRange-1] + rangeOfVals)){
                        return 1;
                    }else{
                        return 0;
                    }
                });

                //Now start with given key and search upwards
                for(int upIndex = key; upIndex < setOfMeanCoords.size(); upIndex++){

                    double[] nextCoord = setOfMeanCoords.get(upIndex);
                    //Continue searching until we have left the acceptable range of values
                    if(Math.abs(centerCoords[dimWithHighRange-1] - nextCoord[dimWithHighRange-1]) > rangeOfVals){
                        break;
                    }else{
                        //Calculate distance and return neighborhood level
                        int cellLevel = determineNeighborhoodLevel(centerCoords, setOfMeanCoords.get(upIndex));
                        if(cellLevel == 1){
                            //Recreate hypercubeID and get its state values
                            double nextCoordID = recreateHypercubeID(nextCoord);
                            Tuple2<Integer, double[]> hypStateValue2 = hypercubeState.get(nextCoordID);
                            //Get unique set of neighbors for LSH search
                            if(!uniqueKeys.contains(nextCoordID)){
                                uniqueKeys.add(nextCoordID);
                            }
                            //Add count from level 1 cell to level1NeighborhoodCount
                            level1NeighborhoodCount += hypStateValue2.f0;
                            totalNeighborhoodCount += hypStateValue2.f0;
                            if(level1NeighborhoodCount >= minPts){
                                potentialOutliers.remove(prunedData);
                                break;
                            }
                        }else if(cellLevel == 2){
                            double nextCoordID = recreateHypercubeID(nextCoord);
                            Tuple2<Integer, double[]> hypStateValue2 = hypercubeState.get(nextCoordID);
                            //Get unique set of neighbors for LSH search
                            if(!uniqueKeys.contains(nextCoordID)){
                                uniqueKeys.add(nextCoordID);
                            }
                            totalNeighborhoodCount += hypStateValue2.f0;
                        }
                    }
                }
                //If the current point still doesn't have minimum neighbors, start searching down
                if(level1NeighborhoodCount < minPts){
                    //Step below given key and search down
                    for(int downIndex = (key-1); downIndex > 0; downIndex--){

                        double[] nextCoord = setOfMeanCoords.get(downIndex);
                        //Continue searching until we have left the acceptable range of values
                        if(Math.abs(centerCoords[dimWithHighRange-1] - nextCoord[dimWithHighRange-1]) > rangeOfVals){
                            break;
                        }else{
                            //Calculate distance and return neighborhood level
                            int cellLevel = determineNeighborhoodLevel(centerCoords, setOfMeanCoords.get(downIndex));
                            if(cellLevel == 1){
                                //Recreate hypercubeID and get its state values
                                double nextCoordID = recreateHypercubeID(nextCoord);
                                Tuple2<Integer, double[]> hypStateValue2 = hypercubeState.get(nextCoordID);
                                //Get unique set of neighbors for LSH search
                                if(!uniqueKeys.contains(nextCoordID)){
                                    uniqueKeys.add(nextCoordID);
                                }
                                //Add count from level 1 cell to level1NeighborhoodCount
                                level1NeighborhoodCount += hypStateValue2.f0;
                                totalNeighborhoodCount += hypStateValue2.f0;
                                if(level1NeighborhoodCount >= minPts){
                                    potentialOutliers.remove(prunedData);
                                    break;
                                }
                            }else if(cellLevel == 2){
                                double nextCoordID = recreateHypercubeID(nextCoord);
                                Tuple2<Integer, double[]> hypStateValue2 = hypercubeState.get(nextCoordID);
                                //Get unique set of neighbors for LSH search
                                if(!uniqueKeys.contains(nextCoordID)){
                                    uniqueKeys.add(nextCoordID);
                                }
                                totalNeighborhoodCount += hypStateValue2.f0;
                            }
                        }
                    }
                }
                //If less than minPts is in all level 1 and 2 cells, the data point is guaranteed to be an outlier
                if(totalNeighborhoodCount < minPts){
                    potentialOutliers.remove(prunedData);
                    collector.collect(prunedData);
                }
            }else{
                potentialOutliers.remove(prunedData);
            }
        }


        //Generate LSH model using all neighbors of questionableData and then get an approximate result for each data point
        if(potentialOutliers.size() > 0){
            //Start off by getting all neighbors for each likelyOutlier
            ArrayList<double[]> setOfNeighPoints = new ArrayList<>();
            for(double theseNeighs : uniqueKeys){
                setOfNeighPoints.addAll(setOfDataPoints.get(theseNeighs));
            }
            //Pass query (current data point) and neighbors to LSH
            double hashFunctions = Math.log(setOfNeighPoints.size());
            int KValue;
            if(hashFunctions % 1 >= 0.5){
                KValue = (int) Math.ceil(hashFunctions);
            }else{
                KValue = (int) Math.floor(hashFunctions);
            }
            MPLSH LSH = new MPLSH(dimensions, 3, KValue, radius);
            for(double[] training : setOfNeighPoints){
                LSH.put(training, training);
            }

            for(Hypercube hypercubePoint : potentialOutliers){
                double[] potentialOutliers = hypercubePoint.coords;
                Neighbor[] approxNeighbors = LSH.knn(potentialOutliers, minPts);
                if(approxNeighbors.length < minPts){
                    collector.collect(hypercubePoint);
                }
            }
        }

        long time_final = System.currentTimeMillis();
        cpuTime += (time_final - time_init);
        numberIterations += 1;
        System.out.println("Total time: " + (time_final - time_init));
        System.out.println("Average time: " + (cpuTime / numberIterations));

        //Clean up states to ensure the program does not get bogged down by traversing information like HypercubeStates that do not have any data points in the current window
        setOfDataPoints.clear();
        potentialOutliers.clear();
        hypercubeNeighs.clear();
        uniqueKeys.clear();
        hypercubeState.clear();
        setOfMeanCoords.clear();

    }


    private int determineNeighborhoodLevel(double[] centerCell, double[] potentialNeighbor){

        //Calculate distance function
        double distance = 0;
        for(int currIndex = 0; currIndex < centerCell.length; currIndex++){
            distance += Math.pow(centerCell[currIndex] - potentialNeighbor[currIndex], 2);
        }
        distance = Math.sqrt(distance);
        double upperBound = distance + (radius/2);
        double lowerBound = distance - (radius/2);

        //If value + and - (diagonal/2) is less than radius, level 1
        if(upperBound < radius && lowerBound < radius){
            return 1;
        }
        //If one value is less than radius and the other is greater, level 2
        else if((upperBound > radius && lowerBound < radius) || (upperBound < radius && lowerBound > radius)){
            return 2;
        }else{
            //If both are greater, cell is out of neighborhood
            return 0;
        }
    }

    private double recreateHypercubeID(double[] meanCoordinates){
        String uniqueID = "";
        for(double currVal : meanCoordinates){
            int ceiling = (int) Math.ceil(currVal);
            int floor = (int) Math.floor(currVal);
            uniqueID += Integer.toString(Math.abs(ceiling));
            uniqueID += Integer.toString(Math.abs(floor));
        }
        return Double.parseDouble(uniqueID);
    }


}
