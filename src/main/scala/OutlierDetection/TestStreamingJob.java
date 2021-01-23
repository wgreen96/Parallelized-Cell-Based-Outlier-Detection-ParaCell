package OutlierDetection;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Collector;
import java.util.*;
import org.apache.flink.table.api.bridge.scala.*;

import static org.apache.flink.table.api.Expressions.$;

public class TestStreamingJob {

    public static void main(String[] args) throws Exception {

        String delimiter = ",";
        String line_delimiter = "&";
        int partitions = 1;
        String myInput = "C:/Users/wgree//Git/PROUD/data/STK/input_20k.txt";
        String dataset = "STK";
        double common_R = 0.35;

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tblEnv = StreamTableEnvironment.create(env);

        DataStream<HypercubePoint> dataStream = env
                .readTextFile(myInput)
                .map((line) -> {
                    String[] cells = line.split(line_delimiter);
                    String[] stringCoords = cells[1].split(delimiter);
                    double[] coords = Arrays.stream(stringCoords).mapToDouble(Double::parseDouble).toArray();
                    long timestamp = Long.parseLong(cells[0]);
                    return new HypercubePoint(coords, timestamp);
                });

        DataStream<HypercubePoint> newData =
                dataStream.map((record) -> {
                    return HypercubeGeneration.createPartitions(partitions, record);
                });


        //Table table1 = tblEnv.fromDataStream(newData);
        Table table1 = tblEnv.fromDataStream(newData, $("coords"), $("arrival"), $("hypercubeID"), $("partitionID"));
        String[] colNames = table1.getSchema().getFieldNames();
        DataType[] colTypes = table1.getSchema().getFieldDataTypes();

        for(String name:colNames){
            System.out.println(name);
        }
        for(DataType vars:colTypes){
            System.out.println(vars);
        }

        env.execute("Java Streaming Job");
    }

}


