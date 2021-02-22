package OutlierDetection;

import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

public class ParallelOutlierDetection extends KeyedBroadcastProcessFunction<Integer, Hypercube, Tuple4<String, Integer, Long, Integer>, Hypercube> {

//    MapState<String, Tuple2<Integer, Integer>> hypercubeState;
//    //State to store when a state was modified
//    MapState<String, Long> lastModification;
//    //State to store what the index for each hypercubestate is for later modifications
//    MapState<String, Integer> cellIndices;

    private final MapStateDescriptor<String, Tuple2<Integer, Integer>> HypercubeStateDesc =
            new MapStateDescriptor<>(
                    "HypercubeState",
                    BasicTypeInfo.STRING_TYPE_INFO,
                    TupleTypeInfo.getBasicTupleTypeInfo(Integer.class, Integer.class));

    // identical to our ruleStateDescriptor above
    private final MapStateDescriptor<String, Long> LastModificationDesc =
            new MapStateDescriptor<>(
                    "LastMoficiation",
                    BasicTypeInfo.STRING_TYPE_INFO,
                    BasicTypeInfo.LONG_TYPE_INFO);






    @Override
    public void processElement(Hypercube hypercube, ReadOnlyContext readOnlyContext, Collector<Hypercube> collector) throws Exception {
        System.out.println("Partition ID: " + hypercube.partitionID);
        ReadOnlyBroadcastState<String, Tuple2<Integer, Integer>> hypercubeState = readOnlyContext.getBroadcastState(HypercubeStateDesc);
//        final MapState<String, Tuple2<Integer, Integer>> hypercubeState = getRuntimeContext().getMapState(HypercubeStateDesc);
        for(Map.Entry<String, Tuple2<Integer, Integer>> val : hypercubeState.immutableEntries()){
            System.out.println("Count: " + val.getKey());
            System.out.println("Hyp PartitionID: " + val.getValue());
        }
    }

    @Override
    public void processBroadcastElement(Tuple4<String, Integer, Long, Integer> cellInfo, Context context, Collector<Hypercube> collector) throws Exception {

        String currHypID = cellInfo.f0;
        int currHypCount = cellInfo.f1;
        long currTime = cellInfo.f2;
        int partitionID = cellInfo.f3;

        BroadcastState<String, Tuple2<Integer, Integer>> hypercubeState = context.getBroadcastState(HypercubeStateDesc);
        BroadcastState<String, Long> lastModification = context.getBroadcastState(LastModificationDesc);


        //Check if the state is new
        if(!hypercubeState.contains(currHypID)){
            //If new, create entry for HypercubeState, lastModified, and setOfCenterCoords
            Tuple2<Integer, Integer> temp = hypercubeState.get(currHypID);
            //hypercubeState.put(currHypID, currHypCount);
            //context.applyToKeyedState(hypercubeState.put(currHypID, temp));
            //hypercubeState.put(currHypID, temp);
            context.getBroadcastState(HypercubeStateDesc).put(currHypID, temp);
            lastModification.put(currHypID, currTime);
        }else{
            //Do error checking for out of order data points by only updating state if data point is newer
            if(currTime > lastModification.get(currHypID)){
                Tuple2<Integer, Integer> temp = new Tuple2<>(currHypCount, partitionID);
                //hypercubeState.put(currHypID, currHypCount);
                //hypercubeState.put(currHypID, temp);
                context.getBroadcastState(HypercubeStateDesc).put(currHypID, temp);
                //lastModification.put(currHypID, currTime);
            }
        }
    }
}
