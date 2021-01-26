package OutlierDetection;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;

import java.util.Map;

//public class CellSummaryCreation extends KeyedProcessFunction<Integer, HypercubePoint, Iterable<Map.Entry<Integer, Integer>>> {
public class CellSummaryCreation extends KeyedProcessFunction<Integer, HypercubePoint, Tuple3<Double, Integer, Long>> {

    static StreamExecutionEnvironment env;
    static StreamTableEnvironment tblEnv;

    /** The state that is maintained by this process function */
    private MapState<Double, Integer> state;

    @Override
    public void open(Configuration parameters) throws Exception {
        state = getRuntimeContext().getMapState(new MapStateDescriptor<>("Hypercube Count", Double.class, Integer.class));
    }

    @Override
    public void processElement(
            HypercubePoint hypercubePoint,
            Context context,
            Collector<Tuple3<Double, Integer, Long>> collector) throws Exception {

        //Parse hypercubeID
        double currHypID = hypercubePoint.hypercubeID;
        //Check if that hypercubeID exists in MapState
        if(state.contains(currHypID)){
            //True, increment value associated with ID
            int newVal = state.get(currHypID) + 1;
            state.put(currHypID, newVal);
        }else{
            //False, create key, value pair
            state.put(currHypID, 1);
        }



        //TODO Create function to check for pruning data and change value for current key if data is pruned

        long currTime = context.timerService().currentWatermark();

        Tuple3<Double, Integer, Long> tester = new Tuple3<Double, Integer, Long>(currHypID, state.get(currHypID), currTime);
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


