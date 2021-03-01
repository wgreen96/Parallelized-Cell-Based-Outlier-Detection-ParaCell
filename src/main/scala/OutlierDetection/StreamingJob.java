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
        int dimWithLargeestRangeOfValues = 0;
        int minPts = 0;
        double radius = 0.0;
        int dimensions = 0;
        String myInput = "";
        int partitions = 8;

        //Window sizes used: 10000, 100000
        //Slide sizes used: 500, 2000, 5000, 20000
        long windowSize = 100000;
        long slideSize = 20000;
        //Query type 0: Only query data points about to expire
        //Type 1: Query data points at middle and end of life
        //Type 2: Query every data point for every window
        int queryType = 2;
        //LSHType type 0: Build LSH once and use that to find an approximate result for all potential outliers
        //Type 1: Build Build LSH once every time a potential outlier is discovered and return and approximate result
        //LSHType is used in an older version of OutlierDetection. Does not have any affect on the current version
        int lshType = 1;
        String outputFile = "/home/green/Documents/testOutputApacheFlink.txt";
        String dataset = "TAO";

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




        env.execute("Java Streaming Job");
    }

    public static void setParameters(int dim, double hypSide, int partition, long wSize, long sSize, int k, double r, int dimToSort, int queryType,int lshType){
        HypercubeGeneration.dimensions = dim;
        HypercubeGeneration.hypercubeSide = hypSide;
        HypercubeGeneration.partitions = partition;
        CellSummaryCreation.windowSize = wSize;
        OutlierDetection.windowSize = wSize;
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
