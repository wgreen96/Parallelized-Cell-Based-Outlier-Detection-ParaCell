package OutlierDetection;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import smile.neighbor.MPLSH;
import smile.neighbor.Neighbor;

import java.util.*;

public class OutlierDetectionTheSixth extends ProcessAllWindowFunction<Hypercube, Hypercube, TimeWindow> {


    Map<Double, Tuple2> hypercubeState = new HashMap<>();
    Map<Double, Long> lastModification = new HashMap<>();
    ArrayList<Hypercube> potentialOutliers = new ArrayList<>();
    ArrayList<ArrayList<Double>> setOfMeanCoords = new ArrayList<>();

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
            long currTime = currPoints.arrival;
            int currHypCount = currPoints.hypercubeCount;


            //Check if the state is new
            if(!hypercubeState.containsKey(currHypID)){
                //If new, create entry for HypercubeState, lastModified, and setOfMeanCoords
                Tuple2<Integer, ArrayList<Double>> hypercubeStateValue = new Tuple2<>(currHypCount, currPoints.centerOfCellCoords);
                hypercubeState.put(currHypID, hypercubeStateValue);
                lastModification.put(currHypID, currTime);
                setOfMeanCoords.add(currPoints.centerOfCellCoords);
            }else{
                //Do error checking for out of order data points by only updating state if data point is newer
                if(currTime > lastModification.get(currHypID)){
                    Tuple2<Integer, ArrayList<Double>> hypercubeStateValue = new Tuple2<>(currHypCount, currPoints.centerOfCellCoords);
                    hypercubeState.put(currHypID, hypercubeStateValue);
                    lastModification.put(currHypID, currTime);
                    setOfMeanCoords.add(currPoints.centerOfCellCoords);
                }
            }

            //Finally, collect the list of data points to be pruned
            if((currPoints.arrival + slideSize) > windowEndTime){
                potentialOutliers.add(currPoints);
            }

        }

        //Sort setOfMeanCoords by the dimension in dimWithHighRange
        Collections.sort(setOfMeanCoords, new Comparator<ArrayList<Double>>() {
            @Override
            public int compare(ArrayList<Double> cell1, ArrayList<Double> cell2) {
                if (cell1.get(dimWithHighRange - 1) < cell2.get(dimWithHighRange - 1)) {
                    return -1;
                } else if (cell1.get(dimWithHighRange - 1) > cell2.get(dimWithHighRange - 1)) {
                    return 1;
                } else {
                    //If they are equal, compare count in Cell
                    if(cell1.get(dimensions) < cell2.get(dimensions)){
                        return -1;
                    }
                    else if(cell1.get(dimensions) > cell2.get(dimensions)){
                        return 1;
                    }else{
                        return 0;
                    }
                }
            }
        });

        for(ArrayList<Double> tempVals : setOfMeanCoords){
            System.out.println(tempVals.toString());
        }



//        System.out.println("SIZE: " + potentialOutliers.size());
//        //Run outlier detection on the data points that will be pruned after this window is processed
//        //This is going backwards because data points are being removed and doing so while going forwards breaks everything
//        for(int pruneIndex = potentialOutliers.size() - 1; pruneIndex >= 0; pruneIndex--){
//
//            Hypercube prunedData = potentialOutliers.get(pruneIndex);
//            double currHypID = prunedData.hypercubeID;
//            Tuple2<Integer, ArrayList<Double>> hypStateValue = hypercubeState.get(currHypID);
//            int hypStateCount = hypStateValue.f0;
//            ArrayList<Double> centerCoords = hypStateValue.f1;
//            int level1NeighborhoodCount = 0;
//            int totalNeighborhoodCount = 0;
//
//            if(hypStateCount < minPts){
//
//                //Binary search for meanCoords.dimWithHighRange within range currHyp.dimWithHighRange +- rangeOfVals
//                int key = Collections.binarySearch(setOfMeanCoords, centerCoords, (cell1, cell2) -> {
//                    if(cell1.get(dimWithHighRange - 1) < (cell2.get(dimWithHighRange - 1) - rangeOfVals)){
//                        return -1;
//                    }
//                    else if(cell1.get(dimWithHighRange - 1) > (cell2.get(dimWithHighRange - 1) + rangeOfVals)){
//                        return 1;
//                    }else{
//                        return 0;
//                    }
//                });
//
//                //Now start with given key and search upwards
//                for(int upIndex = key; upIndex < setOfMeanCoords.size(); upIndex++){
//
//                    ArrayList<Double> nextCoord = setOfMeanCoords.get(upIndex);
//                    //Continue searching until we have left the acceptable range of values
//                    if(Math.abs(centerCoords.get(dimWithHighRange - 1) - nextCoord.get(dimWithHighRange - 1)) > rangeOfVals){
//                        break;
//                    }else{
//                        //Calculate distance and return neighborhood level
//                        int cellLevel = determineNeighborhoodLevel(centerCoords, setOfMeanCoords.get(upIndex));
//                        if(cellLevel == 1){
//                            //Recreate hypercubeID and get its state values
//                            double nextCoordID = recreateHypercubeID(nextCoord);
//                            Tuple2<Integer, double[]> hypStateValue2 = hypercubeState.get(nextCoordID);
//                            //Add count from level 1 cell to level1NeighborhoodCount
//                            level1NeighborhoodCount += hypStateValue2.f0;
//                            totalNeighborhoodCount += hypStateValue2.f0;
//                            if(level1NeighborhoodCount >= minPts){
//                                potentialOutliers.remove(prunedData);
//                                break;
//                            }
//                        }else if(cellLevel == 2){
//                            double nextCoordID = recreateHypercubeID(nextCoord);
//                            Tuple2<Integer, double[]> hypStateValue2 = hypercubeState.get(nextCoordID);
//                            //Get unique set of neighbors for LSH search
//                            totalNeighborhoodCount += hypStateValue2.f0;
//                        }
//                    }
//                }
//                //If the current point still doesn't have minimum neighbors, start searching down
//                if(level1NeighborhoodCount < minPts){
//                    //Step below given key and search down
//                    for(int downIndex = (key-1); downIndex > 0; downIndex--){
//
//                        ArrayList<Double> nextCoord = setOfMeanCoords.get(downIndex);
//                        //Continue searching until we have left the acceptable range of values
//                        if(Math.abs(centerCoords.get(dimWithHighRange - 1) - nextCoord.get(dimWithHighRange - 1)) > rangeOfVals){
//                            break;
//                        }else{
//                            //Calculate distance and return neighborhood level
//                            int cellLevel = determineNeighborhoodLevel(centerCoords, setOfMeanCoords.get(downIndex));
//                            if(cellLevel == 1){
//                                //Recreate hypercubeID and get its state values
//                                double nextCoordID = recreateHypercubeID(nextCoord);
//                                Tuple2<Integer, double[]> hypStateValue2 = hypercubeState.get(nextCoordID);
//                                //Add count from level 1 cell to level1NeighborhoodCount
//                                level1NeighborhoodCount += hypStateValue2.f0;
//                                totalNeighborhoodCount += hypStateValue2.f0;
//                                if(level1NeighborhoodCount >= minPts){
//                                    potentialOutliers.remove(prunedData);
//                                    break;
//                                }
//                            }else if(cellLevel == 2){
//                                double nextCoordID = recreateHypercubeID(nextCoord);
//                                Tuple2<Integer, double[]> hypStateValue2 = hypercubeState.get(nextCoordID);
//                                totalNeighborhoodCount += hypStateValue2.f0;
//                            }
//                        }
//                    }
//                }
//                //If less than minPts is in all level 1 and 2 cells, the data point is guaranteed to be an outlier
//                if(totalNeighborhoodCount < minPts){
//                    potentialOutliers.remove(prunedData);
//                    collector.collect(prunedData);
//                }
//            }else{
//                potentialOutliers.remove(prunedData);
//            }
//        }



        long time_final = System.currentTimeMillis();
        cpuTime += (time_final - time_init);
        numberIterations += 1;
        System.out.println("Total time: " + (time_final - time_init));
        System.out.println("Average time: " + (cpuTime / numberIterations));

        //Clean up states to ensure the program does not get bogged down by traversing information like HypercubeStates that do not have any data points in the current window
        potentialOutliers.clear();
        hypercubeState.clear();
        setOfMeanCoords.clear();

    }


    private int determineNeighborhoodLevel(ArrayList<Double> centerCell, ArrayList<Double> potentialNeighbor){

        //Calculate distance function
        double distance = 0;
        for(int currIndex = 0; currIndex < centerCell.size(); currIndex++){
            distance += Math.pow(centerCell.get(currIndex) - potentialNeighbor.get(currIndex), 2);
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

    private double recreateHypercubeID(ArrayList<Double> meanCoordinates){
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
