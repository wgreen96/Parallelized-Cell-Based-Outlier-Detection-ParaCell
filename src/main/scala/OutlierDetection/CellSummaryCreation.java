package OutlierDetection;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;

public class CellSummaryCreation extends ProcessWindowFunction<HypercubePoint, Table, Integer, TimeWindow> {

    static StreamExecutionEnvironment env;
    static StreamTableEnvironment tblEnv;

    public CellSummaryCreation(){

    }

    @Override
    public void process(Integer key, Context context, Iterable<HypercubePoint> in, Collector<Table> out) throws Exception {



    }


}
