package OutlierDetection;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import smile.neighbor.MPLSH;
import smile.neighbor.Neighbor;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.*;

public class OutlierDetection extends ProcessAllWindowFunction<Hypercube, Hypercube, TimeWindow> {


    Map<String, Integer> hypercubeState = new HashMap<>();
    //State to store when a state was modified
    Map<String, Long> lastModification = new HashMap<>();
    //State to store what the index for each hypercubestate is for later modifications. The modification is the last column of centerCoords, the updated hypercube count
    Map<String, Integer> cellIndices = new HashMap<>();
    ArrayList<Hypercube> potentialOutliers = new ArrayList<>();
    //State to store center coordinate for each hypercube
    ArrayList<ArrayList<Double>> setOfCenterCoords = new ArrayList<>();

    static long slideSize;
    static int minPts;
    static int dimensions;
    static double radius;
    static double hypercubeSide;
    static int dimWithHighRange;
    static int queryType;
    int truePositive = 0;
    int falsePositive = 0;
    int trueNegative = 0;
    int falseNegative = 0;
    int numberIterations = 0;
    long cpuTime = 0L;

    @Override
    public void process(Context context,
                        Iterable<Hypercube> windowPoints,
                        Collector<Hypercube> collector) throws Exception {

        //Get window time
        long windowEndTime = context.window().getEnd();
        long windowStartTime = context.window().getStart();
        long time_init = System.currentTimeMillis();
        int windowSize = 0;

        ArrayList<double[]> paraCellOutliers = new ArrayList<>();
        ArrayList<double[]> nestedLoopOutliers = new ArrayList<>();
        ArrayList<double[]> notOutliers = new ArrayList<>();
//        ArrayList<double[]> notOutliers1 = new ArrayList<>();
//        ArrayList<double[]> notOutliers2 = new ArrayList<>();


        //Start off by iterating through the current window
        for(Hypercube currPoints: windowPoints){

            windowSize++;
            String currHypID = currPoints.hypercubeID;
            long currTime = currPoints.arrival;
            int currHypCount = currPoints.hypercubeCount;
            int currPartitionID = currPoints.partitionID;

            //TODO Make this cleaner
            if(currPartitionID == -1){
                //Check if the state is new
                if(hypercubeState.containsKey(currHypID)){
                    //Do error checking for out of order data points by only updating state if data point is newer
                    if(currTime > lastModification.get(currHypID) || currTime <= (windowStartTime+slideSize)){
                        hypercubeState.put(currHypID, currHypCount);
                        lastModification.put(currHypID, currTime);
                        int indexToReplace = cellIndices.get(currHypID);
                        //Set val at the end instead of adding currPoints.centerOfCellCoords because this kind of data point doesnt have centerOfCellCoords
                        ArrayList<Double> valToUpdate = setOfCenterCoords.get(indexToReplace);
                        int arrayListSize = valToUpdate.size();
                        valToUpdate.set(arrayListSize-1, (double) currHypCount);
                        setOfCenterCoords.set(indexToReplace, valToUpdate);
                    }
                }
            }else{
                windowSize++;
                //Check if the state is new
                if(!hypercubeState.containsKey(currHypID)){
                    //If new, create entry for HypercubeState, lastModified, and setOfCenterCoords
                    hypercubeState.put(currHypID, currHypCount);
                    lastModification.put(currHypID, currTime);
                    setOfCenterCoords.add(currPoints.centerOfCellCoords);
                    cellIndices.put(currHypID, setOfCenterCoords.size()-1);
                }else{
                    //Do error checking for out of order data points by only updating state if data point is newer
                    if(currTime > lastModification.get(currHypID) || currTime <= (windowStartTime+slideSize)){
                        hypercubeState.put(currHypID, currHypCount);
                        lastModification.put(currHypID, currTime);
                        int indexToReplace = cellIndices.get(currHypID);
                        setOfCenterCoords.set(indexToReplace, currPoints.centerOfCellCoords);
                    }
                }


                //Query type where data points are only checked before they are removed
                if(queryType == 0){
                    //Finally, collect the list of data points to be pruned
                    if((currPoints.arrival + slideSize) > windowEndTime){
                        potentialOutliers.add(currPoints);
                    }
                }
                //Query type where data points are checked at the middle and end of their existence
                else if(queryType == 1){
                    if((currPoints.arrival + slideSize) > windowEndTime || ((currPoints.arrival + windowSize)/2) > windowEndTime){
                        potentialOutliers.add(currPoints);
                    }
                }
                //Query type where every data point is checked for every window
                else if(queryType == 2){
                    potentialOutliers.add(currPoints);
                }else{
                    System.out.println("Query type specified does not exist");
                    System.exit(-1);
                }

            }

        }

        //Sort setOfCenterCoords by the dimension in dimWithHighRange. Then sort by the last index which stores the number of data points in a hypercube
        Collections.sort(setOfCenterCoords, new Comparator<ArrayList<Double>>() {
            @Override
            public int compare(ArrayList<Double> cell1, ArrayList<Double> cell2) {
                if (cell1.get(dimWithHighRange - 1) < cell2.get(dimWithHighRange - 1)) {
                    return -1;
                } else if (cell1.get(dimWithHighRange - 1) > cell2.get(dimWithHighRange - 1)) {
                    return 1;
                } else {
                    //If they are equal, compare count in Cell
                    //The inverse is returned here as it makes the first index have the highest count which means the hashmap can simply
                    //add the first unique value is sees rather than constantly comparing this value or waiting on the last one
                    if(cell1.get(dimensions) < cell2.get(dimensions)){
                        return 1;
                    }
                    else if(cell1.get(dimensions) > cell2.get(dimensions)){
                        return -1;
                    }else{
                        return 0;
                    }
                }
            }
        });

        //Map to store the index of start of each value. Used for quickly checking neighbors with highest cell count
        Map<Double, Integer> sortedArrayIndex = new HashMap<>();
        for(int arrIndex = 0; arrIndex < setOfCenterCoords.size(); arrIndex++){
            ArrayList<Double> tempArrayList = setOfCenterCoords.get(arrIndex);
            if(!sortedArrayIndex.containsKey(tempArrayList.get(dimWithHighRange - 1))){
                sortedArrayIndex.put(tempArrayList.get(dimWithHighRange - 1), arrIndex);
            }
        }


        //Run outlier detection on the data points that will be pruned after this window is processed
        for(int pruneIndex = potentialOutliers.size() - 1; pruneIndex >= 0; pruneIndex--){

            Hypercube prunedData = potentialOutliers.get(pruneIndex);
            String currHypID = prunedData.hypercubeID;
            int cellDataCount = hypercubeState.get(currHypID);
            ArrayList<Double> meanCoords = prunedData.centerOfCellCoords;
            int level1Count = 0;

            if(cellDataCount < minPts){
                //Start off by getting the rangeOfVals, it is used to winnow the search space. The range is 1 as that includes all level 1 neighbors except a signle special case
                ArrayList<Integer> valsIndex = new ArrayList<>();
                ArrayList<Double> dimVals = new ArrayList<>();

                //Check if the sortedArrayIndex contains that key value. If so grab the value(starting index of value) associated with that key value
                if(sortedArrayIndex.containsKey(meanCoords.get(dimWithHighRange - 1) - 1)){
                    dimVals.add(meanCoords.get(dimWithHighRange - 1) - 1);
                    valsIndex.add(sortedArrayIndex.get(meanCoords.get(dimWithHighRange - 1) - 1));
                }
                if(sortedArrayIndex.containsKey(meanCoords.get(dimWithHighRange - 1))){
                    dimVals.add(meanCoords.get(dimWithHighRange - 1));
                    valsIndex.add(sortedArrayIndex.get(meanCoords.get(dimWithHighRange - 1)));
                }
                if(sortedArrayIndex.containsKey(meanCoords.get(dimWithHighRange - 1) + 1)){
                    dimVals.add(meanCoords.get(dimWithHighRange - 1) + 1);
                    valsIndex.add(sortedArrayIndex.get(meanCoords.get(dimWithHighRange - 1) + 1));
                }

                boolean stop = false;
                int searchIndex = 0;
                while(!stop){
                    //Iterate through current values in ArrayList. As values are winnowed because they run out of data points or a threshold is met, remove them and the size is decremented
                    int currentSize = valsIndex.size();
                    //Increment through each value index with for loop. Removing elements in loop so it needs to decrement
                    for(int j = currentSize-1; j >= 0; j--){
                        int loopIndex = valsIndex.get(j) + searchIndex;
                        ArrayList<Double> currCell = setOfCenterCoords.get(loopIndex);
                        //Start off by checking we are still looking at data points with the same value
                        double rangeValue = dimVals.get(j);
                        double currCellDimValue = currCell.get(dimWithHighRange-1);
                        if(rangeValue == currCellDimValue){
                            //Then we search the up to the top 300 cells
                            if(searchIndex < 100){
                                //Call distance calculation to check if this is a level 1 neighbor
                                int level = determineNeighborhoodLevel(meanCoords, currCell);
                                //If true, add up count of data points in level 1 neighborhood
                                if(level == 1){
                                    String recreatedID = recreateHypercubeID(currCell);
                                    level1Count += hypercubeState.get(recreatedID);
                                    //Check if count has reached minPts
                                    if(level1Count >= minPts){
                                        potentialOutliers.remove(prunedData);
                                        notOutliers.add(prunedData.coords);
                                        //notOutliers1.add(prunedData.coords);
                                        //Switch boolean to break while loop and immediately break for loop
                                        stop = true;
                                        break;
                                    }
                                }
                            }else{
                                //Stop checking at this point as further cells have too few data points in their area to be worth checking
                                valsIndex.remove(valsIndex.get(j));
                            }
                        }else{
                            //Stop checking as we have moved beyond the index of that data value
                            valsIndex.remove(valsIndex.get(j));
                        }
                    }
                    //Break the loop as we have run out of indices to compare
                    if(valsIndex.size() == 0){
                        stop = true;
                    }else{
                        //Check upcoming index of last value index before continuing
                        if((valsIndex.get(valsIndex.size()-1) + searchIndex + 1) >= setOfCenterCoords.size()){
                            //Stop checking as we have reached the last index of setOfMeans
                            valsIndex.remove(valsIndex.get(valsIndex.size()-1));
                        }
                    }
                    //Increment search index to retrieve the next data point for each value in dimVals
                    searchIndex++;
                }
            }else{
                //Greater than minPts, known not to be an outlier
                potentialOutliers.remove(prunedData);
                notOutliers.add(prunedData.coords);
                //notOutliers1.add(prunedData.coords);
            }
        }

        //Generate LSH model using all neighbors of questionableData and then get an approximate result for each data point
        if(potentialOutliers.size() > 0){
            //Pass query (current data point) and neighbors to LSH
            double hashFunctions = Math.log(windowSize);
            int KValue;
            if(hashFunctions % 1 >= 0.5){
                KValue = (int) Math.ceil(hashFunctions);
            }else{
                KValue = (int) Math.floor(hashFunctions);
            }
            if(KValue < 1){
                KValue = 1;
            }
            MPLSH LSH = new MPLSH(dimensions, 1, KValue, radius*4);
            for(Hypercube currPoints: windowPoints){
                if(currPoints.partitionID != -1){
                    LSH.put(currPoints.coords, currPoints.coords);
                }
            }
            for(Hypercube hypercubePoint : potentialOutliers){
                double[] queryPoint = hypercubePoint.coords;
                Neighbor[] approxNeighbors = LSH.knn(queryPoint, minPts);
                if(approxNeighbors.length < minPts){
                    collector.collect(hypercubePoint);
                    paraCellOutliers.add(hypercubePoint.coords);
                }else{
                    notOutliers.add(hypercubePoint.coords);
                    //notOutliers2.add(hypercubePoint.coords);
                }
            }
        }

//        //Loop to compare outliers from both programs to compute true/false positive
//        nestedLoopOutliers = compareAccuracyWithNestedLoop(windowPoints);
//        for(double[] outlier : paraCellOutliers){
//            if(nestedLoopOutliers.contains(outlier)){
//                truePositive++;
//            }else{
//                falsePositive++;
//            }
//        }
//
//        //Loop to compare non-outliers from ParaCell to Nested Loop to compute true/false negatives
//        for(double[] normalData : notOutliers){
//            if(nestedLoopOutliers.contains(normalData)){
//                falseNegative++;
//            }else{
//                trueNegative++;
//            }
//        }

//        int false1 = 0;
//        //Loop to compare non-outliers from ParaCell to Nested Loop to compute true/false negatives
//        for(double[] normalData : notOutliers1){
//            if(nestedLoopOutliers.contains(normalData)){
//                falseNegative++;
//                false1++;
//            }else{
//                trueNegative++;
//            }
//        }
//
//        int false2 = 0;
//        //Loop to compare non-outliers from ParaCell to Nested Loop to compute true/false negatives
//        for(double[] normalData : notOutliers2){
//            if(nestedLoopOutliers.contains(normalData)){
//                falseNegative++;
//                false1++;
//            }else{
//                trueNegative++;
//            }
//        }

//        System.out.println(false1);
//        System.out.println(false2);

        long time_final = System.currentTimeMillis();
        cpuTime += time_final - time_init;
        numberIterations++;
        System.out.println("Total time: " + (time_final - time_init));
        System.out.println("Average time: " + (cpuTime / numberIterations));
        System.out.println("Iteration: " + numberIterations);
        //System.out.println("Window size:" + windowSize);
        //Query type 2 is the only one to check the entire set of data points in each window like the nested loop function created below
        if(queryType == 2){
//            System.out.println("True Positive: " + truePositive);
//            System.out.println("False Positive: " + falsePositive);
//            System.out.println("True Negative: " + trueNegative);
//            System.out.println("False Negative: " + falseNegative);
        }


        //Clean up states to ensure the program does not get bogged down by traversing information like HypercubeStates that do not have any data points in the current window
        potentialOutliers.clear();
        hypercubeState.clear();
        lastModification.clear();
        setOfCenterCoords.clear();
        cellIndices.clear();

    }


    private int determineNeighborhoodLevel(ArrayList<Double> centerCell, ArrayList<Double> potentialNeighbor){

        //Calculate distance function
        double distance = 0;
        for(int currIndex = 0; currIndex < centerCell.size(); currIndex++){
            distance += Math.pow(centerCell.get(currIndex) - potentialNeighbor.get(currIndex), 2);
        }
        distance = Math.sqrt(distance) * hypercubeSide;
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

    private String recreateHypercubeID(ArrayList<Double> meanCoordinates){
        String uniqueID = "";
        // - 1 to account for count added as last dimension
        for(int index = 0; index < meanCoordinates.size() - 1; index++){
            double currVal = meanCoordinates.get(index);
            int ceiling = (int) Math.ceil(currVal);
            int floor = (int) Math.floor(currVal);
            uniqueID += Integer.toString(Math.abs(ceiling));
            uniqueID += Integer.toString(Math.abs(floor));
        }
        return uniqueID;
    }

    private ArrayList<double[]> compareAccuracyWithNestedLoop(Iterable<Hypercube> windowPoints) {

        ArrayList<double[]> allOutliers = new ArrayList<>();
        for (Hypercube currPoint : windowPoints) {
            if(currPoint.partitionID != -1){
                double[] outliers = currPoint.coords;
                int nearCounter = 0;
                for (Hypercube possibleNeighbors : windowPoints) {
                    if(possibleNeighbors.partitionID != -1){
                        double[] outliers2 = possibleNeighbors.coords;
                        double distance = 0;
                        for (int currIndex = 0; currIndex < outliers.length; currIndex++) {
                            distance += Math.pow(outliers[currIndex] - outliers2[currIndex], 2);
                        }
                        distance = Math.sqrt(distance);
                        if (distance <= radius) {
                            nearCounter++;
                        }
                    }
                }
                if (nearCounter - 1 < minPts) {
                    allOutliers.add(currPoint.coords);
                }
            }
        }
        return allOutliers;
    }


}


//            System.out.println("Average Accuracy: " + avgAccuracy);
//            System.out.println("Overall Correct: " + ((double)totalCorrect/(double)totalOverall)*100);
//FileWriter temp = new FileWriter("/home/green/Documents/OutputTimes.txt", true);
//            BufferedWriter writer = new BufferedWriter(temp);
//            writer.write("Total time: " + (time_final - time_init) + "\n");
//            writer.write("Average time: " + (cpuTime / numberIterations) + "\n");
//            writer.write("Outliers: " + totalOutliers + "\n");
//            writer.close();
//            temp.close();


//            FileWriter temp = new FileWriter("/home/green/Documents/OutputConfusionMatrix.txt", true);
//            BufferedWriter writer = new BufferedWriter(temp);
//            writer.write("True Positive: " + truePositive + "\n");
//            writer.write("False Positive: " + falsePositive + "\n");
//            writer.write("True Negative: " + trueNegative + "\n");
//            writer.write("False Negative: " + falseNegative + "\n");
//            writer.close();
//            temp.close();