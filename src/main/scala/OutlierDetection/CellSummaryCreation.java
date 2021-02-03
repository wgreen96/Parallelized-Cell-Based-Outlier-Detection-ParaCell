package OutlierDetection;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.commons.collections.IteratorUtils;


public class CellSummaryCreation extends KeyedProcessFunction<Integer, Hypercube, Hypercube> {

    //State stores (HypercubeID, count of data points with HypercubeID)
    private MapState<Double, Integer> hypercubeState;
    //State stores (HypercubeID, time before data point is pruned and state.value should be decremented)
    private MapState<Double, LinkedList> timeState;
    private MapState<Double, Integer> testState;

    //The amount of time after processing that a data point can live. Is measured in milliseconds
    static long lifeThreshold;

    @Override
    public void open(Configuration parameters) throws Exception {
        hypercubeState = getRuntimeContext().getMapState(new MapStateDescriptor<>("Hypercube Count", Double.class, Integer.class));
        timeState = getRuntimeContext().getMapState(new MapStateDescriptor<>("Time left for data points", Double.class, LinkedList.class));
        testState = getRuntimeContext().getMapState(new MapStateDescriptor<>("testy", Double.class, Integer.class));
    }


    @Override
    public void processElement(
            Hypercube currPoint,
            Context context,
            Collector<Hypercube> collector) throws Exception {


        //Parse hypercubeID
        double currHypID = currPoint.hypercubeID;
        //Check if that hypercubeID exists in MapState
        if(hypercubeState.contains(currHypID)){
            //True, increment value associated with ID
            int newVal = hypercubeState.get(currHypID) + 1;
            hypercubeState.put(currHypID, newVal);
        }else{
            //False, create key, value pair
            hypercubeState.put(currHypID, 1);
        }


        LinkedList hypercubeQueue;

        //If state already exist, return current time state
        if(timeState.contains(currHypID)){
            hypercubeQueue = timeState.get(currHypID);
        }else{
            hypercubeQueue = new LinkedList<>();
        }

        //Add newest time to time state. Using arrival is not perfect, there will be some deviations within 100ms range in the FIFO queue because of parallel processing
        //That should be fine for many domains and depends on window size, but would need to be revised if its something like high frequency stock trading
        hypercubeQueue.add(currPoint.arrival);

        long currentTime = context.timerService().currentProcessingTime();
        //Check if data points in FIFO queue are to be pruned
        boolean pruning = true;
        while(pruning){
            //Check if head of Queue - currentTime is greater than threshold.
            if(hypercubeQueue.peek() != null && (currentTime - (long) hypercubeQueue.peek()) > lifeThreshold){
                //If so: remove the head, decrement the count, and continue checking
                hypercubeQueue.remove();
                int newVal = hypercubeState.get(currHypID) - 1;
                hypercubeState.put(currHypID, newVal);
            }else{
                //If not, stop checking. No further elements can be removed because any element after the head is unlikely to be newer
                pruning = false;
            }
        }

        //Add new state of hypercubeQueue to Map
        timeState.put(currHypID, hypercubeQueue);

        //Return state with HypercubeID, count to be processed by OutlierDetection function
        Hypercube newPoint = new Hypercube(currPoint.coords, currPoint.arrival, currPoint.hypercubeID,
                                                    currPoint.hyperoctantID, currPoint.partitionID,
                                                    currPoint.meanMultis, hypercubeState.get(currHypID));

        collector.collect(newPoint);

    }
}

//public class CellSummaryCreation extends KeyedProcessFunction<Integer, HypercubePoint, Tuple2<Double, Integer>> {
//    public void processElement(
//            HypercubePoint hypercubePoint,
//            Context context,
//            Collector<Tuple2<Double, Integer>> collector) throws Exception {

//Tuple2<Double, Integer> stateOfHypercube = new Tuple2<Double, Integer>(currHypID, hypercubeState.get(currHypID));
//        collector.collect(stateOfHypercube);

