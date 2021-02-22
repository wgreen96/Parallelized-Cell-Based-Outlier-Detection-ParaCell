package OutlierDetection;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.ArrayList;
import java.util.LinkedList;


public class SerialCellSummaryCreation extends KeyedProcessFunction<Integer, Hypercube, Hypercube> {

    //State stores (HypercubeID, count of data points with HypercubeID)
    private MapState<String, Tuple4<String, Integer, Long, Integer>> hypercubeState;
    //State stores (HypercubeID, time before data point is pruned and state.value should be decremented)
    private MapState<String, LinkedList> timeState;
    final OutputTag<Tuple4<String, Integer, Long, Integer>> outputTag = new OutputTag<>("side-output"){};


    //The amount of time after processing that a data point can live. Is measured in milliseconds
    static long windowSize;

    @Override
    public void open(Configuration parameters) throws Exception {
        MapStateDescriptor<String, Tuple4<String, Integer, Long, Integer>> hypState = new MapStateDescriptor<>(
                "modelState",
                BasicTypeInfo.STRING_TYPE_INFO,
                TupleTypeInfo.getBasicTupleTypeInfo(String.class, Integer.class, Long.class, Integer.class));
        hypercubeState = getRuntimeContext().getMapState(hypState);
        timeState = getRuntimeContext().getMapState(new MapStateDescriptor<>("Time left for data points", String.class, LinkedList.class));
    }


    @Override
    public void processElement(
            Hypercube currPoint,
            Context context,
            Collector<Hypercube> collector) throws Exception {

        long timeInit = System.currentTimeMillis();
        //Parse hypercubeID
        String currHypID = currPoint.hypercubeID;
        //Check if that hypercubeID exists in MapState. If so
        if(hypercubeState.contains(currHypID)){
            //True, increment value and timestamp associated with ID
            Tuple4<String, Integer, Long, Integer> currState = hypercubeState.get(currHypID);
            String id = currState.f0;
            int newVal = currState.f1 + 1;
            long newTime = currState.f2 + 1;
            int pID = currState.f3;
            hypercubeState.put(currHypID, new Tuple4<String, Integer, Long, Integer>(id, newVal, newTime, pID));
        }else{
            //False, create key, value pair
            int newVal = 1;
            long newTime = context.timerService().currentProcessingTime();
            int pID = getRuntimeContext().getIndexOfThisSubtask();
            hypercubeState.put(currHypID, new Tuple4<String, Integer, Long, Integer>(currHypID, newVal, newTime, pID));
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
            if(hypercubeQueue.peek() != null && (currentTime - (long) hypercubeQueue.peek()) > windowSize){
                //If so: remove the head, decrement the count, and continue checking
                hypercubeQueue.remove();
                Tuple4<String, Integer, Long, Integer> currState = hypercubeState.get(currHypID);
                currState.f1 = currState.f1 - 1;
                hypercubeState.put(currHypID, currState);
            }else{
                //If not, stop checking. No further elements can be removed because any element after the head is unlikely to be newer
                pruning = false;
            }
        }

        //Add new state of hypercubeQueue to Map
        timeState.put(currHypID, hypercubeQueue);
        Tuple4<String, Integer, Long, Integer> currState = hypercubeState.get(currHypID);

        //Add currStateCount to centerOfMeanCoords for sorting in Outlier Detection
        ArrayList<Double> centerCoordsWithCount = currPoint.getCenterOfCellCoords();
        centerCoordsWithCount.add(currState.f1.doubleValue());

        //System.out.println("Current ID: " + currHypID + ", Current Time: " + currState.f1);

        //Return state with HypercubeID, count to be processed by OutlierDetection function
        Hypercube newPoint = new Hypercube(currPoint.coords, currState.f2, currPoint.hypercubeID,
                currPoint.partitionID,
                centerCoordsWithCount, currState.f1);

        long timeOut = System.currentTimeMillis();
        //System.out.println(timeOut-timeInit);

        collector.collect(newPoint);
        context.output(outputTag, currState);

    }
}

