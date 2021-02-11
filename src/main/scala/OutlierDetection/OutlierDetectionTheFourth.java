package OutlierDetection;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import smile.neighbor.MPLSH;
import smile.neighbor.Neighbor;

import java.util.*;

public class OutlierDetectionTheFourth extends ProcessAllWindowFunction<Hypercube, Hypercube, TimeWindow> {


    Map<Double, Tuple2> hypercubeState = new HashMap<Double, Tuple2>();
    Map<Tuple2, Integer> hyperOctantState = new HashMap<Tuple2, Integer>();
    Map<Double, Long> lastModification = new HashMap<>();
    Map<Double, ArrayList> setOfDataPoints = new HashMap<Double, ArrayList>();
    ArrayList<Hypercube> dataToBePruned = new ArrayList<>();
    ArrayList<Hypercube> likelyOutliers = new ArrayList<>();
    //ArrayList<double[]> outliers = new ArrayList<>();
    ArrayList<Double> uniqueKeys = new ArrayList<>();
    Map<Double, Tuple2> hypercubeNeighs = new HashMap<>();

    static long windowSize;
    static long slideSize;
    static int kNeighs;
    static int dimensions;
    static double radius;
    long cpuTime = 0L;
    double numberIterations = 0;
    int counts = 0;


    @Override
    public void process(Context context,
                        Iterable<Hypercube> windowPoints,
                        Collector<Hypercube> collector) throws Exception {


        long time_init = System.currentTimeMillis();

        //Get window time
        long windowStartTime = context.window().getStart();
        long windowEndTime = context.window().getEnd();
        //System.out.println(windowStartTime + ", " + windowEndTime);

        //Start off by iterating through the current window
        for(Hypercube currPoints: windowPoints){

            double currHypID = currPoints.hypercubeID;
            double currHypOctID = currPoints.hyperoctantID;
            long currTime = currPoints.arrival;
            int currHypCount = currPoints.hypercubeCount;
            double[] currHypMeanCoords = currPoints.centerOfCellCoords;


//            if(currTime < windowStartTime || currTime > windowEndTime){
//                //System.out.println(currTime);
//                //System.out.println(windowStartTime + ", " + windowEndTime + ", " + time_init);
//                //System.out.println(currTime);
//                System.out.println("SHOULDNT HAPPEN");
//                System.out.println((currTime - windowStartTime) + ", " + (windowEndTime - currTime));
//
//            }else{
//                System.out.println("SHOULD HAPPEN");
//                System.out.println((currTime - windowStartTime) + ", " + (windowEndTime - currTime));
//            }
//            if(currTime > windowEndTime){
//                //System.out.println("SHOULDNT HAPPEN");
//                counts++;
//                System.out.println(counts);
//            }


            //Check if the state is new
            if(!hypercubeState.containsKey(currHypID)){
                //If new, create entry for HypercubeState and lastModified
                Tuple2<Integer, double[]> hypercubeStateValue = new Tuple2<>(currHypCount, currHypMeanCoords);
                hypercubeState.put(currHypID, hypercubeStateValue);
                lastModification.put(currHypID, currTime);
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
            if(!setOfDataPoints.containsKey(currHypID)){
                newList = new ArrayList<>();

            }else{
                newList = setOfDataPoints.get(currHypID);
            }
            newList.add(currPoints.coords);
            setOfDataPoints.put(currHypID, newList);

            //Finally, check if data points will be pruned before the next slide
            //Why <= instead of >?
//            if((currPoints.arrival + windowSize) <= windowEndTime){
//                dataToBePruned.add(currPoints);
//            }
            if((currPoints.arrival + slideSize) > windowEndTime){
                dataToBePruned.add(currPoints);
            }

        }

        for(Hypercube prunedData : dataToBePruned){

            double currHypID = prunedData.hypercubeID;
            Tuple2<Integer, double[]> hypStateValue = hypercubeState.get(currHypID);
            int hypStateCount = hypStateValue.f0;
            double[] centerCoords = hypStateValue.f1;

            //Start Outlier Detection by checking if current cell is less than k
            if(hypStateCount < kNeighs){

                //Need to return sum of level 1 neighbors. Start by parsing hypercubeID
                int level1NeighborhoodCount = hypStateCount;
                int totalNeighborhoodCount = hypStateCount;
                ArrayList<Double> setOfNeighs = new ArrayList<>();

                //If hypercubeNeighs state already exists, we know the neighbors. Do a simple loop and count for each
                //TODO WASNT THIS AN ISSUE FOR SOME REASON?
                if(hypercubeNeighs.containsKey(currHypID)){
                    Tuple2<ArrayList, Integer> neighborhoodState = hypercubeNeighs.get(currHypID);
                    ArrayList<Double> keyNeighs =  neighborhoodState.f0;
                    level1NeighborhoodCount = neighborhoodState.f1;
                    for(double neighs: keyNeighs){
                        int stateCount = (int) hypercubeState.get(neighs).f0;
                        totalNeighborhoodCount += stateCount;
                    }
                }
                //Else, need to loop through current set of hypercubes and create state
                else{
                    //Compare ID of current Hypercube to the rest of hypercubes
                    for(Double currCubes: hypercubeState.keySet()) {
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
                            //Get unique set of neighbors for LSH search
                            if(!uniqueKeys.contains(currCubes)){
                                uniqueKeys.add(currCubes);
                            }
                            setOfNeighs.add(currCubes);
                            int thisCellCount = hypStateValue2.f0;
                            level1NeighborhoodCount += thisCellCount;
                            if(level1NeighborhoodCount >= kNeighs){
                                break;
                            }
                        }
                        //If one value is less than radius and the other is greater, level 2
                        else if((upperBound > radius && lowerBound < radius) || (upperBound < radius && lowerBound > radius)){
                            //Get unique set of neighbors for LSH search
                            if(!uniqueKeys.contains(currCubes)){
                                uniqueKeys.add(currCubes);
                            }
                            setOfNeighs.add(currCubes);
                        }
                        //If both are greater, cell is out of neighborhood

                    }
                    //Add setOfNeighs to hypercubeNeighs
                    hypercubeNeighs.put(currHypID, new Tuple2<ArrayList, Integer>(setOfNeighs, level1NeighborhoodCount));

                }


                //If level 1 neighbors still isn't enough to reach k, add data point to list of data points that still need processing
                if(level1NeighborhoodCount < kNeighs){
                    //If the total neighborhood, 1 and 2, is less than kNeighs then it is guarenteed to be an Outlier
                    if(totalNeighborhoodCount < kNeighs){
//                        outliers.add(prunedData.coords);
//                        collector.collect(prunedData.coords);
                        //collector.collect(prunedData);
                    }else{
//                        likelyOutliers.add(prunedData.coords);
                        likelyOutliers.add(prunedData);
                    }

                }
            }
        }


//        //Generate LSH model using all neighbors of questionableData and then get an approximate result for each data point
//        if(likelyOutliers.size() > 0){
//            //Start off by getting all neighbors for each likelyOutlier
//            ArrayList<double[]> setOfNeighPoints = new ArrayList<>();
//            for(double theseNeighs : uniqueKeys){
//                setOfNeighPoints.addAll(setOfDataPoints.get(theseNeighs));
//            }
//            //Pass query (current data point) and neighbors to LSH
//            double hashFunctions = Math.log(setOfNeighPoints.size());
//            int KValue = 0;
//            if(hashFunctions % 1 >= 0.5){
//                KValue = (int) Math.ceil(hashFunctions);
//            }else{
//                KValue = (int) Math.floor(hashFunctions);
//            }
//            MPLSH LSH = new MPLSH(dimensions, 3, KValue, radius);
//            for(double[] training : setOfNeighPoints){
//                LSH.put(training, training);
//            }
//
//            for(Hypercube hypercubePoint : likelyOutliers){
//                double[] potentialOutliers = hypercubePoint.coords;
//                Neighbor[] approxNeighbors = LSH.knn(potentialOutliers, kNeighs);
//                if(approxNeighbors.length < kNeighs){
//                    //outliers.add(potentialOutliers);
//                    collector.collect(hypercubePoint);
//                }
//            }
//        }


        if(likelyOutliers.size() > 0) {
            for(Hypercube hypOutliers : likelyOutliers) {
                double[] outliers = hypOutliers.coords;
                int nearCounter = 0;
                for (Hypercube hypOutliers2 : likelyOutliers) {
                    double[] outliers2 = hypOutliers2.coords;
                    double distance = 0;
                    for (int currIndex = 0; currIndex < outliers.length; currIndex++) {
                        distance += Math.pow(outliers[currIndex] - outliers2[currIndex], 2);
                    }
                    distance = Math.sqrt(distance);
                    if (distance <= radius) {
                        nearCounter++;
                    }
                }
                if(nearCounter - 1 < kNeighs){
                    collector.collect(hypOutliers);
                }
                //System.out.println("NESTED LOOP NEIGHS: " + (nearCounter - 1));
            }
        }

        long time_final = System.currentTimeMillis();
        cpuTime += (time_final - time_init);
        numberIterations += 1;

        //Clean up states to ensure the program does not get bogged down by traversing information like HypercubeStates that do not have any data points in the current window
        setOfDataPoints.clear();
        dataToBePruned.clear();
        likelyOutliers.clear();
        hypercubeNeighs.clear();
        uniqueKeys.clear();
        hypercubeState.clear();
        hyperOctantState.clear();


//        System.out.println(cpuTime);
//        System.out.println(numberIterations);
        //System.out.println("Average time: " + (cpuTime/numberIterations));
        //System.out.println("NUM OUTLIERS: " + outliers.size());
        //System.out.println("Outliers part2 : " + outliers.size());
//        outliers.clear();


    }
}


//            for(double[] outliers:likelyOutliers){
//                int nearCounter = 0;
//                for(double[] outliers2:likelyOutliers){
//                    double distance = 0;
//                    for(int currIndex = 0; currIndex < outliers.length; currIndex++){
//                        distance += Math.pow(outliers[currIndex] - outliers2[currIndex], 2);
//                    }
//                    distance = Math.sqrt(distance);
//                    if(distance <= radius){
//                        nearCounter++;
//                    }
//                }
//                //System.out.println("NESTED LOOP NEIGHS: " + (nearCounter - 1));
//            }