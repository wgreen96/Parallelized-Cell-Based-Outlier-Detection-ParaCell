package OutlierDetection;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

import java.util.LinkedList;

public class OutlierDetection extends CoProcessFunction<Tuple2<Double, Integer>, Hypercube, String> {

    //State stores (HypercubeID, count of data points with HypercubeID), shared by both streams
    private MapState<Double, Integer> cellSummaries;
    //State stores (HypercubeID, processing time), is used to determine when OutlierDetection is called
    private MapState<Double, LinkedList> timeState;
    static long lifeThreshold;

    @Override
    public void open(Configuration config) {
        cellSummaries = getRuntimeContext().getMapState(new MapStateDescriptor<>("Hypercube Count", Double.class, Integer.class));
        timeState = getRuntimeContext().getMapState(new MapStateDescriptor<>("Time left for data points", Double.class, LinkedList.class));
    }

    //Function associated with building the Cell Summary aggregation state
    @Override
    public void processElement1(Tuple2<Double, Integer> cellStates, Context context, Collector<String> collector) throws Exception {
        //Update cellSummaries with new count for key in .f0
        cellSummaries.put(cellStates.f0, cellStates.f1);
    }

    //Outlier Detection function
    @Override
    public void processElement2(Hypercube hypercubePoint, Context context, Collector<String> collector) throws Exception {

        LinkedList hypercubeQueue;
        //Parse hypercubeID
        double currHypID = hypercubePoint.hypercubeID;
        //If state already exist, return current time state
        if(timeState.contains(currHypID)){
            hypercubeQueue = timeState.get(currHypID);
        }else{
            hypercubeQueue = new LinkedList<>();
        }

        //Add newest time to time state. Using arrival is not perfect, there will be some deviations within 100ms range in the FIFO queue because of parallel processing
        hypercubeQueue.add(hypercubePoint.arrival);

        long currentTime = context.timerService().currentProcessingTime();
        //Check if data points in FIFO queue are to be pruned
        boolean pruning = true;
        while(pruning){
            //Check if head of Queue - currentTime is greater than threshold.
            if(hypercubeQueue.peek() != null && (currentTime - (long) hypercubeQueue.peek()) > lifeThreshold){
                //If so: remove the head, run outlier detection
                hypercubeQueue.remove();
                //OUTLIER DETECTION
                //Need to create table first
            }else{
                //If not, stop checking. No further elements can be removed because any element after the head is unlikely to be newer
                pruning = false;
            }
        }
    }
}
