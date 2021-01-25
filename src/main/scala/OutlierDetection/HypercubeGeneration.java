package OutlierDetection;


import java.util.ArrayList;

public class HypercubeGeneration {

    static double dimensions;
    static double radius;
    static double diagonal;
    static double hypercubeSide;
    static int partitions;


    public void HypercubeGeneration(){

    }

    public static HypercubePoint createPartitions(HypercubePoint dataPoint){
        //Create data structure to store values that will become HypercubeID
        ArrayList<Integer> multiplicationValues = new ArrayList<>();
        //For each coordinate
        for(Double val : dataPoint.coords) {
            //Find closest multiple of hypercubeSide
            double closestMultiple = val / hypercubeSide;
            //Store int values to create unique id for Hypercube
            multiplicationValues.add((int) Math.ceil(closestMultiple));
            multiplicationValues.add((int) Math.floor(closestMultiple));
        }
        //Set hypercubeID and partitionID
        int newHypercubeID = createHypercubeID(multiplicationValues);
        int newPartitionID = newHypercubeID % partitions;
        return new HypercubePoint(dataPoint.coords, dataPoint.arrival, newHypercubeID, newPartitionID);
    }

    public static int createHypercubeID(ArrayList<Integer> multiplicationVals){
        //Concatenate int values
        //So {1,4,6,2,5} creates uniqueID 14625
        int uniqueID = 0;
        int arraySize = multiplicationVals.size();
        for(int currIndex = 0; currIndex < arraySize; currIndex++){
            uniqueID += Math.pow(10, ((arraySize - 1) - currIndex)) * multiplicationVals.get(currIndex);
        }
        return uniqueID;
    }

}
//METHOD FOR STORING HYPERCUBEID, PARTITIONID IN HASKMAP, MAY BE USEFUL LATER ON IF CURRENT METHOD IS UNBALANCED
//Was not used because, at least with 20k data points, it returned even partition results. Could be different with more data
//    static Map<Integer, Integer> IDMap = new HashMap<Integer, Integer>();
//    static int partitionCounter = 0;

//If the key already exists
//        if(IDMap.containsKey(newHypercubeID)){
//            //Set HypercubeID for data point and get partitionID from HashMap
//            dataPoint.setHypercubeID(newHypercubeID);
//            dataPoint.setPartitionId(IDMap.get(newHypercubeID));
//        }
//        else{
//            //Create partition ID
//            int newPartitionID = partitionCounter % partitions;
//            partitionCounter++;
//            //Store hypercubeID and partitionID in HashMap
//            IDMap.put(newHypercubeID, newPartitionID);
//            //Set values for data point
//            dataPoint.setHypercubeID(newHypercubeID);
//            dataPoint.setPartitionId(newPartitionID);
//        }


//METHOD FOR GETTING A HYPERCUBES BOUNDARY COORDINATES
//public static Buffer<Double> createPartitions(int partitions, Data_hypercube dataPoint){
//Create data structure to store hypercube boundary coordinates
//        ArrayList<Double> coordBoundaries = new ArrayList<>();
//            //Use ceiling and floor to get the 2 closest ints
//            double topVal = hypercubeSide * Math.ceil(closestMultiple);
//            double bottomVal = hypercubeSide * Math.floor(closestMultiple);
//            coordBoundaries.add(topVal);
//            coordBoundaries.add(bottomVal);
//Before returning value, go back to Scala data structure
//        Buffer<Double> coordVals = JavaConversions.asScalaBuffer(coordBoundaries);