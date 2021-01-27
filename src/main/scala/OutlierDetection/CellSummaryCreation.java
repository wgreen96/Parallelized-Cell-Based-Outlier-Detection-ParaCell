package OutlierDetection;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import java.util.LinkedList;


//public class CellSummaryCreation extends KeyedProcessFunction<Integer, HypercubePoint, Iterable<Map.Entry<Integer, Integer>>> {
public class CellSummaryCreation extends KeyedProcessFunction<Integer, HypercubePoint, Tuple2<Double, Integer>> {

    //State stores (HypercubeID, count of data points with HypercubeID)
    private MapState<Double, Integer> hypercubeState;
    //State stores (HypercubeID, time before data point is pruned and state.value should be decremented)
    private MapState<Double, LinkedList> timeState;

    //The amount of time after processing that a data point can live. Is measured in milliseconds
    static long lifeThreshold = 100;

    @Override
    public void open(Configuration parameters) throws Exception {
        hypercubeState = getRuntimeContext().getMapState(new MapStateDescriptor<>("Hypercube Count", Double.class, Integer.class));
        timeState = getRuntimeContext().getMapState(new MapStateDescriptor<>("Time left for data points", Double.class, LinkedList.class));
    }

    @Override
    public void processElement(
            HypercubePoint hypercubePoint,
            Context context,
            Collector<Tuple2<Double, Integer>> collector) throws Exception {

        //Parse hypercubeID
        double currHypID = hypercubePoint.hypercubeID;
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
        //Get time for data point currently being processed and place in Queue
        long currentTime = context.timerService().currentProcessingTime();
        //If state already exist, return current time state
        if(timeState.contains(currHypID)){
            hypercubeQueue = timeState.get(currHypID);
        }else{
            hypercubeQueue = new LinkedList<>();
        }
        //Add newest time to time state
        hypercubeQueue.add(currentTime);

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
                //If not, stop checking. We are assured no further elements can be removed because any element after the head is newer
                pruning = false;
            }
        }

        //Add new state of hypercubeQueue to Map
        timeState.put(currHypID, hypercubeQueue);

        //Return state with HypercubeID, count to be processed by OutlierDetection function
        Tuple2<Double, Integer> stateOfHypercube = new Tuple2<Double, Integer>(currHypID, hypercubeState.get(currHypID));
        collector.collect(stateOfHypercube);

    }


//    @Override
//    public void onTimer(
//            long timestamp,
//            OnTimerContext ctx,
//            Collector<Tuple2<Integer, Integer>> out) throws Exception {
//
//        // retrieve the current count
//        Tuple2<Integer, Integer> current = state.entries();
//
//        // emit the state
//        out.collect(current);
//
//    }

}


