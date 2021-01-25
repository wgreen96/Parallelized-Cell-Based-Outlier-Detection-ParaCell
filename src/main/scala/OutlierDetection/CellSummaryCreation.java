package OutlierDetection;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;

import java.util.Map;

//public class CellSummaryCreation extends KeyedProcessFunction<Integer, HypercubePoint, Iterable<Map.Entry<Integer, Integer>>> {
public class CellSummaryCreation extends KeyedProcessFunction<Integer, HypercubePoint, Tuple2<Integer, Integer>> {

    static StreamExecutionEnvironment env;
    static StreamTableEnvironment tblEnv;

    /** The state that is maintained by this process function */
    private MapState<Integer, Integer> state;

    @Override
    public void open(Configuration parameters) throws Exception {
        state = getRuntimeContext().getMapState(new MapStateDescriptor<>("Hypercube Count", Integer.class, Integer.class));
    }

    @Override
    public void processElement(
            HypercubePoint hypercubePoint,
            Context context,
            Collector<Tuple2<Integer, Integer>> collector) throws Exception {

//        // retrieve the current count
//        MapState<Integer, Integer> current = state;

        //Parse hypercubeID
        int currHypID = hypercubePoint.hypercubeID;
        //Check if that hypercubeID exists in MapState
        if(state.contains(currHypID)){
            //True, increment value associated with ID
            int newVal = state.get(currHypID) + 1;
            state.put(currHypID, newVal);
        }else{
            //False, create key, value pair
            state.put(currHypID, 1);
        }

        //Create event to return results after some condition

        // schedule the next timer 60 seconds from the current event time
        //60000 is 60 seconds
        //TODO Come back to this later, hopefully with a better idea than simply waiting on a timer
        //I think that is an issue because I am returning a single tuple every time, so waiting on a timer would ensure I miss those
        //that are not modified when the time reaches its condition
//        context.timerService().registerEventTimeTimer(1000);
        Tuple2<Integer, Integer> tester = new Tuple2<Integer, Integer>(currHypID, state.get(currHypID));
        collector.collect(tester);

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


