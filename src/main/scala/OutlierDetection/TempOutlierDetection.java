package OutlierDetection;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class TempOutlierDetection extends ProcessFunction<Hypercube, String> {

    //State stores (HypercubeID, count of data points with HypercubeID)
    private MapState<Double, Integer> hypercubeState;
    //State stores (HypercubeID, time before data point is pruned and state.value should be decremented)
    private MapState<Double, LinkedList> timeState;
    //State stores (Tuple2<HypercubeID, HyperOctantID>, count)
    private MapState<Tuple2, Integer> hyperOctantState;
    //State stores sorted HypercubeIDs
    private ListState<Double> sortedIDs;

    //k neigbors required to be nonoutlier
    static int kNeighs;
    //The amount of time after processing that a data point can live. Is measured in milliseconds
    static long lifeThreshold;

    @Override
    public void open(Configuration parameters) throws Exception {
        hypercubeState = getRuntimeContext().getMapState(new MapStateDescriptor<>("Hypercube Count", Double.class, Integer.class));
        hyperOctantState = getRuntimeContext().getMapState(new MapStateDescriptor<>("Hypercube Count", Tuple2.class, Integer.class));
        timeState = getRuntimeContext().getMapState(new MapStateDescriptor<>("Time left for data points", Double.class, LinkedList.class));
        sortedIDs = getRuntimeContext().getListState(new ListStateDescriptor<Double>("Sorted list of IDS", Double.class));

    }

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
        if(!hypercubeState.contains(currHypID)){
            newHypercube = true;
        }
        hypercubeState.put(currHypID, currHypCount);

        //Create a sorted list of hypercubes for searching in Outlier Detection
        if(newHypercube == true){
            //Insert hypercubeID in array while keeping a sorted structure
            if(sortedIDs.get() != null){
                Iterable<Double> currIter = sortedIDs.get();
                List<Double> currList = StreamSupport
                        .stream(currIter.spliterator(), false)
                        .collect(Collectors.toList());
                currList.add(currHypID);
                Collections.sort(currList);
                sortedIDs.update(currList);
            } else{
                sortedIDs.add(currHypID);
            }

            System.out.println("START OF LIST!!!!!!!!!!!!!!!!!!!!!!");
            Iterable<Double> teser1 = sortedIDs.get();
            for(Double vals : teser1){
                System.out.println(vals);
            }

        }

        //Update Hyperoctant state
        Tuple2<Double, Double> hyOctantID = new Tuple2<>(currHypID, currHypOctID);
        if(hyperOctantState.contains(hyOctantID)){
            int newVal = hyperOctantState.get(hyOctantID) + 1;
            hyperOctantState.put(hyOctantID, newVal);
        }else{
            hyperOctantState.put(hyOctantID, 1);
        }

        //Update time state
        LinkedList hypercubeQueue;
        //If state already exist, return current time state
        if(timeState.contains(currHypID)){
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

                //Start Outlier Detection by checking is current cell is less than k
                if(hypercubeState.get(currHypID) < kNeighs){
                    //Need to return sum of level 1 neighbors

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
