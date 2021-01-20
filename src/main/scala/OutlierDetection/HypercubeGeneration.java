package OutlierDetection;

import scala.collection.mutable.Buffer;
import scala.collection.mutable.ListBuffer;
import scala.collection.JavaConversions;

import java.util.ArrayList;

public class HypercubeGeneration {

    //Hardcoded values, will need to be changed after this is finished
    static double radius = 5;
    static double diagonal = radius/2;
    static double dimensions = 3;
    static double hypercubeSide = diagonal/Math.sqrt(dimensions);


    public void HypercubeGeneration(){

    }

    //so this works because I took record and returned a single dimension of it
    public static ListBuffer<Object> createPartition(Data_basis vals){
        boolean tester = false;
        ListBuffer<Object> objs = new ListBuffer<>();
        while(!tester){
            objs = vals.value();
            tester = true;
        }
        return objs;
    }

    public static Buffer<Double> createPartitions(int partitions, Data_basis dataPoint){
        //Create data structure to store hypercube boundary coordinates
        ArrayList<Double> coordBoundaries = new ArrayList<>();
        //Get data points coordiates, also need to convert Scala structure to Java alternative
        Iterable<Object> javaDataCoords = JavaConversions.asJavaIterable(dataPoint.value());
        //For each coordinate
        for(Object val: javaDataCoords){
            //Find closest multiple of hypercubeSide
            double closestMultiple = (double) val / hypercubeSide;
            //Use ceiling and floor to get the 2 closest ints
            double topVal = hypercubeSide * Math.ceil(closestMultiple);
            coordBoundaries.add(topVal);
            double bottomVal = hypercubeSide * Math.floor(closestMultiple);
            coordBoundaries.add(bottomVal);
        }
        //Before returning value, go back to Scala data structure
        Buffer<Double> coordVals = JavaConversions.asScalaBuffer(coordBoundaries);
        return coordVals;
    }


}
