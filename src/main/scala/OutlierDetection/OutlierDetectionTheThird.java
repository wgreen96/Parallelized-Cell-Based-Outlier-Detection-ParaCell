package OutlierDetection;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.text.DecimalFormat;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class OutlierDetectionTheThird extends ProcessFunction<Hypercube, String> {

    //If the difference between the aggregate multiplication values of 2 cells is greater than this than they are not level 1 neighbors.
    //So if (radius - (hypercubeSide/2)) = 4.27 and I divide that by hypercubeSide to get 2.96. The multiplication vales of two cells cannot be
    //greater than or equal to rangeOfValues
    static double rangeOfValues;
    //k neigbors required to be nonoutliers
    static int kNeighs;
    //The amount of time after processing that a data point can live. Is measured in milliseconds
    static long lifeThreshold;
    static double dimensions;

    Map<Double, Integer> hypercubeState = new HashMap<Double, Integer>();
    Map<Tuple2, Integer> hyperOctantState = new HashMap<Tuple2, Integer>();
    Map<Double, LinkedList> timeState = new HashMap<Double, LinkedList>();
    ArrayList<Double> sortedIDs = new ArrayList<Double>();
    int dataCounter = 0;

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
                dataCounter++;
                //If so: remove the head, run outlier detection
                hypercubeQueue.remove();
                //Start Outlier Detection by checking if current cell is less than k
                if(hypercubeState.get(currHypID) < kNeighs){
                    //Need to return sum of level 1 neighbors. Start by parsing hypercubeID
                    int totalNeighborhoodCount = hypercubeState.get(currHypID);
                    String stringCurrID = new DecimalFormat("#").format(currHypID);
                    ArrayList<Integer> setOfVals = new ArrayList<>();
                    double[] setOfMulties = new double[(int)dimensions];
                    boolean[] setOfNegatives = new boolean[(int)dimensions];
                    double tempVal = currHypID;
                    int chCount = 0;
                    int indexCount = 0;
                    //Gets last n values from HypercubeID to know how large each multiple is
                    for(int i = 0; i < dimensions; i++){
                        setOfMulties[(int) dimensions - i - 1] = (int) Math.floor(tempVal % 10);
                        tempVal = tempVal / 10;
                    }
                    //Using information from above, parse HypercubeID to get all multiplication values
                    for(int k = 0; k < dimensions; k++){
                        int currentSize = (int) setOfMulties[k];
                        //This is special case where the first multiplication value is 0 and Java will remove the 0, so the size is 1
                        if(currentSize == 1){
                            setOfVals.add(indexCount, 0);
                            setOfVals.add(indexCount+1, 1);
                            setOfNegatives[k] = true;
                            indexCount += 2;
                            chCount++;
                        }else{
                            setOfVals.add(indexCount, Integer.parseInt(stringCurrID.substring(chCount, chCount+currentSize/2)));
                            chCount += currentSize/2;
                            setOfVals.add(indexCount+1, Integer.parseInt(stringCurrID.substring(chCount, chCount+currentSize/2)));
                            indexCount += 2;
                            chCount += currentSize/2;
                            if(setOfVals.get(indexCount-1) > setOfVals.get(indexCount-2)){
                                setOfNegatives[k] = true;
                            }else{
                                setOfNegatives[k] = false;
                            }
                        }
                    }

                    //Compare ID of current Hypercube to the rest of hypercubes
                    for(Double currCubes: sortedIDs){
                        //Skip comparison to self
                        if(currCubes == currHypID){
                            continue;
                        }
                        String stringCube = new DecimalFormat("#").format(currCubes);
                        double[] setOfMulties2 = new double[(int)dimensions];
                        double tempVal2 = currCubes;
                        int chCount2 = 0;
                        int arrayIndex = 0;
                        //Value to track distance between two cells
                        int difference = 0;
                        //Do same process as above to parse multiplication vals
                        for(int i = 0; i < dimensions; i++){
                            setOfMulties2[(int) dimensions - i - 1] = (int) Math.floor(tempVal2 % 10);
                            tempVal2 = tempVal2 / 10;

                        }
                        for(int k = 0; k < dimensions; k++){
                            boolean currNegative = false;
                            int currVal1 = 0;
                            int currVal2 = 0;
                            int currentSize = (int) setOfMulties2[k];
                            if(currentSize == 1){
                                currVal1 = 0;
                                currVal2 = 1;
                                chCount2++;
                                currNegative = true;
                            }else{
                                currVal1 = Integer.parseInt(stringCube.substring(chCount2, chCount2+currentSize/2));
                                chCount2 += currentSize/2;
                                currVal2 = Integer.parseInt(stringCube.substring(chCount2, chCount2+currentSize/2));
                                chCount2 += currentSize/2;
                                if(currVal2 < currVal1){
                                    currNegative = true;
                                }
                            }
                            //If both are negative or both positive, normal comparisons are fine
                            if((setOfNegatives[k] && currNegative) || (!setOfNegatives[k] && !currNegative) ){
                                 difference += (currVal1 - setOfVals.get(arrayIndex)) + (currVal2 - setOfVals.get(arrayIndex+1));
                            }else{
                                 difference += (currVal1 - setOfVals.get(arrayIndex+1)) + (currVal2 - setOfVals.get(arrayIndex));
                            }
                            arrayIndex += 2;

                            if(difference >= rangeOfValues){
                                break;
                            }
                        }

                        int thisCellCount = hypercubeState.get(currCubes);
                        totalNeighborhoodCount += thisCellCount;
                        if(totalNeighborhoodCount >= kNeighs){
                            break;
                        }
                    }

                    //Now we have compared level 1 neighbors. If that still isn't above k, need to get level 2 neighbors
                    if(totalNeighborhoodCount < kNeighs){

                    }
                    System.out.println(totalNeighborhoodCount);

                }



            }else{
                //If not, stop checking. No further elements can be removed because any element after the head is unlikely to be newer
                pruning = false;
            }
        }
        //After doing outlier detection
        timeState.put(currHypID, hypercubeQueue);
        System.out.println(dataCounter);


    }
}
