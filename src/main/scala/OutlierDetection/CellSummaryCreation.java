package OutlierDetection;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.LinkedList;


public class CellSummaryCreation extends KeyedProcessFunction<Integer, Hypercube, Hypercube> {

    //State stores (HypercubeID, count of data points with HypercubeID)
    private MapState<String, Tuple2<Integer, Long>> hypercubeState;
    //State stores (HypercubeID, time before data point is pruned and state.value should be decremented)
    private MapState<String, LinkedList> timeState;

    //The amount of time after processing that a data point can live. Is measured in milliseconds
    static long windowSize;

    @Override
    public void open(Configuration parameters) throws Exception {
        MapStateDescriptor<String, Tuple2<Integer, Long>> hypState = new MapStateDescriptor<>(
                "modelState",
                BasicTypeInfo.STRING_TYPE_INFO,
                TupleTypeInfo.getBasicTupleTypeInfo(Integer.class, Long.class));
        hypercubeState = getRuntimeContext().getMapState(hypState);
        timeState = getRuntimeContext().getMapState(new MapStateDescriptor<>("Time left for data points", String.class, LinkedList.class));
    }


    @Override
    public void processElement(
            Hypercube currPoint,
            Context context,
            Collector<Hypercube> collector) throws Exception {

        //Parse hypercubeID
        String currHypID = currPoint.hypercubeID;
        //Check if that hypercubeID exists in MapState. If so
        if(hypercubeState.contains(currHypID)){
            //True, increment value and timestamp associated with ID
            Tuple2<Integer, Long> currState = hypercubeState.get(currHypID);
            int newVal = currState.f0 + 1;
            long newTime = currState.f1 + 1;
            hypercubeState.put(currHypID, new Tuple2<Integer, Long>(newVal, newTime));
        }else{
            //False, create key, value pair
            int newVal = 1;
            long newTime = context.timerService().currentProcessingTime();
            hypercubeState.put(currHypID, new Tuple2<Integer, Long>(newVal, newTime));
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
                Tuple2<Integer, Long> currState = hypercubeState.get(currHypID);
                currState.f0 = currState.f0 - 1;
                hypercubeState.put(currHypID, currState);
            }else{
                //If not, stop checking. No further elements can be removed because any element after the head is unlikely to be newer
                pruning = false;
            }
        }

        //Add new state of hypercubeQueue to Map
        timeState.put(currHypID, hypercubeQueue);
        Tuple2<Integer, Long> currState = hypercubeState.get(currHypID);

        //Add currStateCount to centerOfMeanCoords for sorting in Outlier Detection
        ArrayList<Double> centerCoordsWithCount = currPoint.getCenterOfCellCoords();
        centerCoordsWithCount.add(currState.f0.doubleValue());

        //System.out.println("Current ID: " + currHypID + ", Current Time: " + currState.f1);

        //Return state with HypercubeID, count to be processed by OutlierDetection function
        Hypercube newPoint = new Hypercube(currPoint.coords, currState.f1, currPoint.hypercubeID,
                currPoint.partitionID,
                centerCoordsWithCount, currState.f0);


        collector.collect(newPoint);

    }
}

//        pruneCounter++;
//        int keyCounter = 0;
//        int prunedVals = 0;
//        long thisCurrentTime = System.currentTimeMillis();
//        //Every 10000 data ponits, go through the state and see if any Hypercubes should be pruned
//        if(pruneCounter % 10000 == 1){
//            for(String keys : hypercubeState.keys()){
//                keyCounter++;
//                LinkedList thisHypercubeQueue = timeState.get(keys);
//                if(thisHypercubeQueue.peek() != null && (thisCurrentTime - (long) thisHypercubeQueue.peek()) > windowSize){
//                    thisHypercubeQueue.remove();
//                    Tuple2<Integer, Long> currState2 = hypercubeState.get(keys);
//                    currState2.f0 = currState2.f0 - 1;
//                    if(currState2.f0 == 0){
//                        hypercubeState.remove(keys);
//                        timeState.remove(keys);
//                        prunedVals++;
//                    }else{
//                        hypercubeState.put(keys, currState2);
//                    }
//                }
//            }
//            System.out.println("Number hypercubes checked: " + keyCounter);
//            System.out.println("Number hypercubes pruned: " + prunedVals);
//        }