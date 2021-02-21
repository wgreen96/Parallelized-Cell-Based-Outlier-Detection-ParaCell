package OutlierDetection;

import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.evictors.Evictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class StreamingJob {

    public static void main(String[] args) throws Exception {

        String delimiter = ",";
        String outputFile = "/home/green/Documents/testOutputApacheFlink.txt";
        //String outputFile = "C:/Users/wgree/Documents/testOutputApacheFlink.txt";
        int partitions = 8;
        long windowSize = 10000;
        long slideSize = 500;

//        int dimWithLargeestRangeOfValues = 2;
//        int minPts = 50;
//        double radius = 1.9;
//        int dimensions = 3;
//        //String myInput = "C:/Users/wgree//Git/PROUD/data/TAO/tree_input.txt";
//        String myInput = "/home/green/Documents/PROUD/data/TAO/tree_input.txt";

//        double radius = 739.0;
//        int minPts = 33;
//        int dimensions = 10;
//        int dimWithLargeestRangeOfValues = 10;
//        //String myInput = "C:/Users/wgree/Git/OutlierThesisDevelopment/ForestCoverTest1.txt";
//        String myInput = "/home/green/Documents/Datasets/ForestCoverTest1.txt";

//        double radius = 13;
//        int minPts = 30;
//        int dimensions = 67;
//        int dimWithLargeestRangeOfValues = 14;
//        //String myInput = "C:/Users/wgree/Git/OutlierThesisDevelopment/ForestCoverTest1.txt";
//        String myInput = "/home/green/Documents/Datasets/FARS.txt";

        double radius = 2000;
        int minPts = 20;
        int dimensions = 90;
        int dimWithLargeestRangeOfValues = 14;
        //String myInput = "C:/Users/wgree/Git/OutlierThesisDevelopment/ForestCoverTest1.txt";
        String myInput = "/home/green/Documents/Datasets/YearPredictionMSD.txt";

        //Generate environment for DataStream and Table API
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(partitions);

        //Calculate the hypercube side for every cell
        double hypercubeSide = (radius/2) / Math.sqrt(dimensions);
        //Set static parameters for Hypercube Generation, Cell Summary Creation, and Outlier Detection
        setParameters(dimensions, hypercubeSide, partitions, windowSize, slideSize, minPts, radius, dimWithLargeestRangeOfValues);


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

//        //Create DataStream using a source
//        DataStream<Hypercube> dataStream = env
//                .readTextFile(myInput)
//                .map((line) -> {
//                    String[] tempLine = line.split(";");
//                    String[] stringCoords = tempLine[1].split(delimiter);
////                    ArrayList<Double> doubleCoords = new ArrayList<>();
////                    for(String stringVal : stringCoords){
////                        doubleCoords.add(Double.parseDouble(stringVal));
////                    }
//                    double[] coords = Arrays.stream(stringCoords).mapToDouble(Double::parseDouble).toArray();
//                    long currTime = System.currentTimeMillis();
//                    Thread.sleep(1);
//                    return new Hypercube(coords, currTime);
//                });


        //Generate HypercubeID and PartitionID for each data object in the stream
        DataStream<Hypercube> dataWithHypercubeID =
                dataStream
                        .map(HypercubeGeneration::createPartitions);

//        //Partition the data by partitionID
//        DataStream<Hypercube> dataWithCellSummaries =
//                dataWithHypercubeID
//                        .keyBy(Hypercube::getKey)
//                        .process(new CellSummaryCreation());

        //Partition the data by partitionID
        DataStream<Hypercube> dataWithCellSummaries =
                dataWithHypercubeID
                        .keyBy(Hypercube::getKey)
                        .process(new CellSummaryCreationSixth());


        DataStream<Hypercube> outliers =
            dataWithCellSummaries
                    .windowAll(SlidingProcessingTimeWindows.of(Time.milliseconds(windowSize), Time.milliseconds(slideSize)))
                    .allowedLateness(Time.milliseconds(slideSize))
                    .process(new OutlierDetectionTheSixth())
                    .setParallelism(1);

        outliers
                .writeAsText(outputFile, FileSystem.WriteMode.OVERWRITE)
                .setParallelism(1);



        env.execute("Java Streaming Job");
    }

    public static void setParameters(int dim, double hypSide, int partition, long wSize, long sSize, int k, double r, int dimToSort){
        HypercubeGeneration.dimensions = dim;
        HypercubeGeneration.hypercubeSide = hypSide;
        HypercubeGeneration.partitions = partition;
        CellSummaryCreationSixth.windowSize = wSize;
        OutlierDetectionTheSixth.slideSize = sSize;
        OutlierDetectionTheSixth.minPts = k;
        OutlierDetectionTheSixth.dimensions = dim;
        OutlierDetectionTheSixth.radius = r;
        OutlierDetectionTheSixth.hypercubeSide = hypSide;
        OutlierDetectionTheSixth.dimWithHighRange = dimToSort;
    }

}



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


//Set parameter values for HypercubeGeneration to calculate the desired atomic hypercube's side length
//        HypercubeGeneration.dimensions = dimensions;
//                HypercubeGeneration.hypercubeSide = hypercubeSide;
//                HypercubeGeneration.partitions = partitions;
////        CellSummaryCreation.windowSize = windowSize;
//                CellSummaryCreationSixth.windowSize = windowSize;
////        OutlierDetectionTheFourth.slideSize = slideSize;
////        OutlierDetectionTheFourth.minPts = minPts;
////        OutlierDetectionTheFourth.dimensions = dimensions;
////        OutlierDetectionTheFourth.radius = radius;
////        OutlierDetectionTheFifth.slideSize = slideSize;
////        OutlierDetectionTheFifth.minPts = minPts;
////        OutlierDetectionTheFifth.dimensions = dimensions;
////        OutlierDetectionTheFifth.radius = radius;
////        OutlierDetectionTheFifth.hypercubeSide = hypercubeSide;
////        OutlierDetectionTheFifth.dimWithHighRange = dimWithLargeestRangeOfValues;
//                OutlierDetectionTheSixth.slideSize = slideSize;
//                OutlierDetectionTheSixth.minPts = minPts;
//                OutlierDetectionTheSixth.dimensions = dimensions;
//                OutlierDetectionTheSixth.radius = radius;
//                OutlierDetectionTheSixth.hypercubeSide = hypercubeSide;
//                OutlierDetectionTheSixth.dimWithHighRange = dimWithLargeestRangeOfValues;