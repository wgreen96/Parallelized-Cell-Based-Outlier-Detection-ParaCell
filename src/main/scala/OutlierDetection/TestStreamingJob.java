package OutlierDetection;

import com.typesafe.sslconfig.ssl.ExpressionSymbol;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Expressions;
import org.apache.flink.table.api.Over;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.FormatDescriptor;
import org.apache.flink.table.descriptors.OldCsv;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.table.sinks.CsvTableSink;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

import java.nio.file.FileSystem;
import java.util.*;

import static org.apache.flink.table.api.Expressions.*;

public class TestStreamingJob {

    public static void main(String[] args) throws Exception {

        String myInput = "/home/green/Documents/PROUD/data/STK/input_20k.txt";
        //String myInput = "C:/Users/wgree//Git/PROUD/data/TAO/input_20k.txt";
        String dataset = "STK";
        String delimiter = ",";
        String line_delimiter = "&";
        double radius = 5;
        double dimensions = 3;
        int partitions = 3;
//        double common_R = 0.35;
//        long windowSize = 10000;
//        long slideSize = 500;
//        double kNeighs = 50;

        //Generate environment for Datastream and Table API
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(partitions);
        StreamTableEnvironment tblEnv = StreamTableEnvironment.create(env);

        //Set parameter values for HypercubeGeneration to calculate the desired atomic hypercube's side length
        HypercubeGeneration.dimensions = dimensions;
        HypercubeGeneration.radius = radius;
        HypercubeGeneration.diagonal = radius / 2;
        HypercubeGeneration.hypercubeSide = HypercubeGeneration.diagonal / Math.sqrt(dimensions);
        HypercubeGeneration.partitions = partitions;

        //Create DataStream using a source
        DataStream<HypercubePoint> dataStream = env
                .readTextFile(myInput)
                .map((line) -> {
                    String[] cells = line.split(line_delimiter);
                    String[] stringCoords = cells[1].split(delimiter);
                    double[] coords = Arrays.stream(stringCoords).mapToDouble(Double::parseDouble).toArray();
                    long timeStamp = Long.parseLong(cells[0]);
                    return new HypercubePoint(coords, timeStamp);
                });

        //Generate HypercubeID and PartitionID for each data object in the stream
        DataStream<HypercubePoint> newData =
                dataStream
                        .map(HypercubeGeneration::createPartitions);


        //Partition the data by partitionID
        DataStream<Tuple2<Double, Integer>> cellSummaries =
                newData
                        .keyBy(HypercubePoint::getKey)
                        .process(new CellSummaryCreation())
                        .setParallelism(partitions);

        cellSummaries
                .print()
                .setParallelism(1);

        env.execute("Java Streaming Job");
    }



}

//        newData.writeAsText("C:/Users/wgree/Documents/testOutputApacheFlink.txt", FileSystem.WriteMode.OVERWRITE);


//        Table table1 = tblEnv.fromDataStream(newData, $("coords"), $("arrival"), $("hypercubeID"), $("partitionID"));
//        String[] colNames = table1.getSchema().getFieldNames();
//        DataType[] colTypes = table1.getSchema().getFieldDataTypes();
//
//        for(String name:colNames){
//            System.out.println(name);
//        }
//        for(DataType vars:colTypes){
//            System.out.println(vars);
//        }


//    Table table1 = tblEnv.fromDataStream(newData, $("coords"), $("arrival"), $("hypercubeID"), $("partitionID"));
//    Table result = tblEnv.sqlQuery(
//            "SELECT partitionID FROM " + table1);
//    // create and register a TableSink
//    final Schema schema = new Schema()
//            .field("partitionID", DataTypes.INT());
//    // this doesnt work
//    tblEnv.connect(new FileSystem().path("C:/Users/wgree/Documents/testOutputApacheFlink.txt"))
//            .withFormat(new OldCsv())
//            .withSchema(schema)
//            .createTemporaryTable("RubberOrders");


//        //Create partitions for counting number of objects in hypercube
//        Table summaryTable = dataTable
//                .window(Over
//                            .partitionBy($("partitionID"))
//                            .orderBy($("arrival"))
//                            .preceding(UNBOUNDED_RANGE)
//                            .following(CURRENT_RANGE)
//                            .as("HypercubeCount"))
//                .select(
//                        $("b").avg().over($("w"))
//                );

//Partition the data by partitionID
//        //Table testTable =
//        newData
//                .keyBy(HypercubePoint::getKey)
//                .window(SlidingProcessingTimeWindows.of(Time.milliseconds(windowSize), Time.milliseconds(slideSize)))
//                .allowedLateness(Time.milliseconds(slideSize))
//                .process(new CellSummaryCreation());

//convert stream into table, .rowtime() converts arrival values in arrival to timestamp values
//        Table dataTable = tblEnv.fromDataStream(newData, $("coords"), $("arrival").rowtime(), $("hypercubeID"), $("partitionID"));

//        //Partition the data by partitionID
//        DataStream<Iterable<Map.Entry<Integer, Integer>>> cellSummaries =
//                newData
//                        .keyBy(HypercubePoint::getKey)
//                        .process(new CellSummaryCreation())
//                        .setParallelism(partitions);

//                .assignTimestampsAndWatermarks(WatermarkStrategy.<HypercubePoint>forMonotonousTimestamps()
//                                                                .withTimestampAssigner((HypercubePoint, timestamp) -> HypercubePoint.getArrival()));