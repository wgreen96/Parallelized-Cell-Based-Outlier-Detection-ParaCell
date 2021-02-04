package OutlierDetection;

import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class OutlierDetectionTheFourth extends ProcessAllWindowFunction<Hypercube, String, TimeWindow> {



    @Override
    public void process(Context context, Iterable<Hypercube> iterable, Collector<String> collector) throws Exception {

    }
}
