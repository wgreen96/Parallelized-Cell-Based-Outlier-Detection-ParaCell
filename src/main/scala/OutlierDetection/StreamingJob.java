package OutlierDetection;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class StreamingJob {

    public static void main(String[] args) throws Exception {

        //String myInput = "/home/green/Documents/PROUD/data/TAO/tree_input.txt";
        String myInput = "C:/Users/wgree//Git/PROUD/data/TAO/tree_input.txt";
        String delimiter = ",";
        double radius = 5;
        int dimensions = 3;
        int partitions = 8;
//        long windowSize = 500;
//        long slideSize = 250;
        long windowSize = 10000;
        long slideSize = 500;
        int kNeighs = 50;

        //Generate environment for DataStream and Table API
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(partitions);

        //Set parameter values for HypercubeGeneration to calculate the desired atomic hypercube's side length
        HypercubeGeneration.dimensions = dimensions;
        HypercubeGeneration.radius = radius;
        HypercubeGeneration.diagonal = radius / 2;
        HypercubeGeneration.hypercubeSide = HypercubeGeneration.diagonal / Math.sqrt(dimensions);
        HypercubeGeneration.partitions = partitions;

        //lifeThreshold (milliseconds) is the amount of time before a data point is pruned.
        CellSummaryCreation.windowSize = windowSize;
        OutlierDetectionTheFourth.windowSize = windowSize;
        OutlierDetectionTheFourth.slideSize = slideSize;
        OutlierDetectionTheFourth.kNeighs = kNeighs;
        OutlierDetectionTheFourth.dimensions = dimensions;
        OutlierDetectionTheFourth.radius = radius;


        //Create DataStream using a source
        DataStream<Hypercube> dataStream = env
                .readTextFile(myInput)
                .map((line) -> {
                    String[] stringCoords = line.split(delimiter);
                    double[] coords = Arrays.stream(stringCoords).mapToDouble(Double::parseDouble).toArray();
                    long currTime = System.currentTimeMillis();
                    return new Hypercube(coords, currTime);
                })
                .setParallelism(1);



        //Generate HypercubeID and PartitionID for each data object in the stream
        DataStream<Hypercube> dataWithHypercubeID =
                dataStream
                        .map(HypercubeGeneration::createPartitions);

        //Partition the data by partitionID
        DataStream<Hypercube> dataWithCellSummaries =
                dataWithHypercubeID
                        .keyBy(Hypercube::getKey)
                        .process(new CellSummaryCreation());

        dataWithCellSummaries
                .windowAll(SlidingProcessingTimeWindows.of(Time.milliseconds(windowSize), Time.milliseconds(slideSize)))
                .process(new OutlierDetectionTheFourth())
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

//        cellSummaries
//                .print()
//                .setParallelism(1);

//        //Partition the data by partitionID
//        DataStream<Tuple2<Double, Integer>> cellSummaries =
//                dataWithHypercubeID
//                        .keyBy(HypercubePoint::getKey)
//                        .process(new CellSummaryCreation());

//        cellSummaries
//                .print()
//                .setParallelism(1);


//        cellSummaries
//                .connect(newData)
//                .process(new OutlierDetection())
//                .setParallelism(1);
