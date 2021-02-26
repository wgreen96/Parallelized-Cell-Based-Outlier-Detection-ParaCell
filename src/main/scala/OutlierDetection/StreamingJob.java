package OutlierDetection;

import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.util.*;

public class StreamingJob {

    public static void main(String[] args) throws Exception {

        String delimiter = ",";
        String outputFile = "/home/green/Documents/testOutputApacheFlink.txt";
        boolean OutlierDetectionParallel = false;
        int dimWithLargeestRangeOfValues = 0;
        int minPts = 0;
        double radius = 0.0;
        int dimensions = 0;
        String myInput = "";
        int partitions = 8;

        long windowSize = 100000;
        long slideSize = 20000;
        int queryType = 2;
        int lshType = 1;
        String dataset = "MSD";

        if(dataset == "TAO"){
            dimWithLargeestRangeOfValues = 2;
            minPts = 50;
            radius = 1.9;
            dimensions = 3;
            myInput = "/home/green/Documents/Datasets/TAO.txt";
        }else if(dataset == "FC"){
            radius = 739.0;
            minPts = 33;
            dimensions = 10;
            dimWithLargeestRangeOfValues = 10;
            myInput = "/home/green/Documents/Datasets/ForestCover.txt";
        }else if(dataset == "FARS") {
            radius = 13;
            minPts = 30;
            dimensions = 67;
            dimWithLargeestRangeOfValues = 14;
            myInput = "/home/green/Documents/Datasets/FARS.txt";
        }else if(dataset == "MSD"){
            radius = 2000;
            minPts = 20;
            dimensions = 90;
            dimWithLargeestRangeOfValues = 14;
            myInput = "/home/green/Documents/Datasets/YearPredictionMSD.txt";
        }else{
            System.out.println("Dataset does not exist");
            System.exit(-1);
        }


        //Generate environment for DataStream and Table API
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(partitions);


        //Calculate the hypercube side for every cell
        double hypercubeSide = (radius/2) / Math.sqrt(dimensions);
        //Set static parameters for Hypercube Generation, Cell Summary Creation, and Outlier Detection
        setParameters(dimensions, hypercubeSide, partitions, windowSize, slideSize, minPts, radius, dimWithLargeestRangeOfValues, queryType, lshType);


        //Create DataStream using a source
        DataStream<Hypercube> dataStream = env
                .readTextFile(myInput)
                .map((line) -> {
                    String[] stringCoords = line.split(delimiter);
                    double[] coords = Arrays.stream(stringCoords).mapToDouble(Double::parseDouble).toArray();
                    //Simulate events arriving once every millisecond
                    Thread.sleep(1);
                    long currTime = System.currentTimeMillis();
                    return new Hypercube(coords, currTime);
                })
                .setParallelism(1);


        //Generate HypercubeID and PartitionID for each data object in the stream
        DataStream<Hypercube> dataWithHypercubeID =
                dataStream
                        .map(HypercubeGeneration::createPartitions)
                        .setParallelism(1);

        //Check if outlier detection is ran in parallel. Greatly affects how the program is built
        if(OutlierDetectionParallel){

            //Partition the data by partitionID
            SingleOutputStreamOperator<Hypercube> dataWithCellCount =
                    dataWithHypercubeID
                            .keyBy(Hypercube::getKey)
                            .process(new SerialCellSummaryCreation());

            //Get information about each cell as side output
            final OutputTag<Tuple4<String, Integer, Long, Integer>> outputTag = new OutputTag<>("side-output"){};
            DataStream<Tuple4<String, Integer, Long, Integer>> cellSummaries =
                    dataWithCellCount.getSideOutput(outputTag);

            //Create a broadcast of that sideoutput so it can be used as a global state when running keyed outlier detection
            MapStateDescriptor<String, Tuple4<String, Integer, Long, Integer>> hypState = new MapStateDescriptor<>(
                    "modelState",
                    BasicTypeInfo.STRING_TYPE_INFO,
                    TupleTypeInfo.getBasicTupleTypeInfo(String.class, Integer.class, Long.class, Integer.class));
            BroadcastStream<Tuple4<String, Integer, Long, Integer>> cellStream = cellSummaries.broadcast(hypState);


            dataWithCellCount
                    .keyBy(Hypercube::getKey)
                    .connect(cellStream)
                    .process(new ParallelOutlierDetection());

        }else{
            //Partition the data by partitionID
            DataStream<Hypercube> dataWithCellSummaries =
                    dataWithHypercubeID
                            .keyBy(Hypercube::getKey)
                            .process(new CellSummaryCreation());

            //Run Outlier Detection on the data stream and return the outliers
            DataStream<Hypercube> outliers =
                    dataWithCellSummaries
                            .windowAll(SlidingProcessingTimeWindows.of(Time.milliseconds(windowSize), Time.milliseconds(slideSize)))
                            .allowedLateness(Time.milliseconds(500))
                            .process(new OutlierDetection())
                            .setParallelism(1);

            //Write outliers to file
            outliers
                    .writeAsText(outputFile, FileSystem.WriteMode.OVERWRITE)
                    .setParallelism(1);
        }



        env.execute("Java Streaming Job");
    }

    public static void setParameters(int dim, double hypSide, int partition, long wSize, long sSize, int k, double r, int dimToSort, int queryType,int lshType){
        HypercubeGeneration.dimensions = dim;
        HypercubeGeneration.hypercubeSide = hypSide;
        HypercubeGeneration.partitions = partition;
        CellSummaryCreation.windowSize = wSize;
        OutlierDetection.slideSize = sSize;
        OutlierDetection.minPts = k;
        OutlierDetection.dimensions = dim;
        OutlierDetection.radius = r;
        OutlierDetection.hypercubeSide = hypSide;
        OutlierDetection.dimWithHighRange = dimToSort;
        OutlierDetection.queryType = queryType;

        OldCellSummaryCreation.windowSize = wSize;
        OldOutlierDetection.slideSize = sSize;
        OldOutlierDetection.minPts = k;
        OldOutlierDetection.dimensions = dim;
        OldOutlierDetection.radius = r;
        OldOutlierDetection.hypercubeSide = hypSide;
        OldOutlierDetection.dimWithHighRange = dimToSort;
        OldOutlierDetection.lshType = lshType;
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