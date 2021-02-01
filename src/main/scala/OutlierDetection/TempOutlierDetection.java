package OutlierDetection;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.LinkedList;

public class TempOutlierDetection extends ProcessAllWindowFunction<Hypercube, String, TimeWindow> {

    //State stores (HypercubeID, count of data points with HypercubeID)
    private MapState<Double, Integer> hypercubeState;
    //State stores (HypercubeID, time before data point is pruned and state.value should be decremented)
    private MapState<Double, LinkedList> timeState;
    //State stores (Tuple2<HypercubeID, HyperOctantID>, count)
    private MapState<Tuple2, Integer> hyperOctantState;

    //The amount of time after processing that a data point can live. Is measured in milliseconds
    static long lifeThreshold;

    @Override
    public void open(Configuration parameters) throws Exception {
        hypercubeState = getRuntimeContext().getMapState(new MapStateDescriptor<>("Hypercube Count", Double.class, Integer.class));
        hyperOctantState = getRuntimeContext().getMapState(new MapStateDescriptor<>("Hypercube Count", Tuple2.class, Integer.class));
        timeState = getRuntimeContext().getMapState(new MapStateDescriptor<>("Time left for data points", Double.class, LinkedList.class));

    }

    @Override
    public void process(
            Context context,
            Iterable<Hypercube> windowPoints,
            Collector<String> collector) throws Exception {

        for(Hypercube currPoint : windowPoints){

            //Update hypercubeState with newest information
            double currHypID = currPoint.hypercubeID;
            double currHypOctID = currPoint.hyperoctantID;
            int currHypCount = currPoint.hypercubeCount;
            hypercubeState.put(currHypID, currHypCount);

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






            //After doing outlier detection
            timeState.put(currHypID, hypercubeQueue);

        }
    }
}
