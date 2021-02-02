package OutlierDetection;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.text.DecimalFormat;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class OutlierDetectionTheThird extends ProcessFunction<Hypercube, String> {

    //k neigbors required to be nonoutlier
    static int kNeighs;
    //The amount of time after processing that a data point can live. Is measured in milliseconds
    static long lifeThreshold;
    static double dimensions;
    static double rangeOfValues;

    Map<Double, Integer> hypercubeState = new HashMap<Double, Integer>();
    Map<Tuple2, Integer> hyperOctantState = new HashMap<Tuple2, Integer>();
    Map<Double, LinkedList> timeState = new HashMap<Double, LinkedList>();
    ArrayList<Double> sortedIDs = new ArrayList<Double>();

    @Override
    public void processElement(
            Hypercube currPoint,
            Context context,
            Collector<String> collector) throws Exception {

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
            //Insert hypercubeID in array while keeping a sorted structure
            sortedIDs.add(currHypID);
            Collections.sort(sortedIDs);
            //System.out.println(currHypID);
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
                    System.out.println(currHypID);
                    ArrayList<Integer> setOfVals = new ArrayList<>();
                    String stringCurrID = new DecimalFormat("#").format(currHypID);
                    double tempVal = currHypID;
                    int chCount = 0;
                    int indexCount = 0;
                    for(int i = 0; i < dimensions; i++){
                        //TODO THIS IS GOING IN REVERSE. IF THE VALUE IS .....122, I going through my loop as 221
                        int currentSize = (int) Math.floor(tempVal % 10);
                        tempVal = tempVal / 10;
                        if(currentSize == 1){
                            setOfVals.add(indexCount, 0);
                            setOfVals.add(indexCount+1, 1);
                            indexCount += 2;
                            chCount++;
                        }else{
                            System.out.println("char count: " + chCount);
                            System.out.println("index count " + indexCount);
                            System.out.println("Size val " + currentSize);
                            setOfVals.add(indexCount, Integer.parseInt(stringCurrID.substring(chCount, chCount+currentSize/2)));
                            chCount += currentSize/2;
                            setOfVals.add(indexCount+1, Integer.parseInt(stringCurrID.substring(chCount, chCount+currentSize/2)));
                            indexCount += 2;
                            chCount += currentSize/2;
                        }
                    }

                    for(int newerVals : setOfVals){
                        System.out.println(newerVals);
                    }

                    //Compare ID of current Hypercube to the rest of hypercubes
//                    for(Double currCubes: sortedIDs){
//                        String stringCube = Double.toString(currCubes);
//                        double tempVal2 = currHypID;
//                        int charCount = 0;
//                        for(int i = 0; i < dimensions; i++){
                            //TODO THIS IS GOING IN REVERSE
//                            int currentSize = (int) Math.floor(tempVal2 % 10);
//                            tempVal2 = tempVal2 / 10;
//                            //Compare values
//                            int firstVal = Integer.parseInt(stringCube.substring(charCount,charCount+currentSize));
//                            charCount += currentSize;
//                            int secondVal = Integer.parseInt(stringCube.substring(charCount,charCount+currentSize));
//                            charCount += currentSize;
//
//                        }
//                    }



                }



            }else{
                //If not, stop checking. No further elements can be removed because any element after the head is unlikely to be newer
                pruning = false;
            }
        }
        //After doing outlier detection
        timeState.put(currHypID, hypercubeQueue);

    }
}
