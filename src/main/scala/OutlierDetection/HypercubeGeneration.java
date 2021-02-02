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

    public static Hypercube createPartitions(Hypercube dataPoint){
        //Create data structure to store values that will become HypercubeID
        ArrayList<Double> multiplicationValues = new ArrayList<>();
        double[] arrayOfIDs = new double[2];
        //For each coordinate
        for(Double val : dataPoint.coords) {
            //Find closest multiple of hypercubeSide. 0.0001 is included to stop really small values from being concatenated to 0.0
            double closestMultiple = val / hypercubeSide + 0.0001;
            //Store values to create unique id for Hypercube
            multiplicationValues.add(closestMultiple);
        }
        //Set hypercubeID and partitionID
        arrayOfIDs = createIDs(multiplicationValues);
        double newHypercubeID = arrayOfIDs[0];
        double newHyperoctantID = arrayOfIDs[1];
        int newPartitionID = (int) (Math.abs(newHypercubeID) % partitions);
        return new Hypercube(dataPoint.coords, dataPoint.arrival, newHypercubeID, newHyperoctantID, newPartitionID);
    }

    public static double[] createIDs(ArrayList<Double> multiplicationVals){
        boolean negative;
        String uniqueID = "";
        //Value to parse UniqueID later
        String uniqueIDSize = "";
        int expCounter = 0;
        double hyperOctID = 0;
        double[] idStorage = new double[2];

        //Concatenate int values
        //So {1.3,4.7,63.1} creates uniqueID 21546463
        for(double currValue : multiplicationVals){
            negative = false;
            //Check if coordinate is negative
            if(currValue < 0){ negative = true; }

            //Ceiling and floor are to ensure any values in range of those 2 end up with same hypercubeID
            int ceiling = (int) Math.ceil(currValue);
            int floor = (int) Math.floor(currValue);
            //This creates uniqueID for the same positive and negative number. (ex = 4.1) 4.1 = 54, -4.1 = 45
            uniqueID += Integer.toString(Math.abs(ceiling));
            uniqueID += Integer.toString(Math.abs(floor));
            //Special case, Java ignores 0 if it is first value
            if(ceiling == 0){
                uniqueIDSize += 1;
            }else if(Math.abs(currValue) < 10) {
                uniqueIDSize += 2;
            }else{
                uniqueIDSize += Integer.toString((int) (Math.floor(Math.log10(Math.abs(currValue))) + 1) * 2);
            }

            //System.out.println("Coord: " + currValue + ", ceiling: " + ceiling + ", floor: " + floor);

            //Find HyperOctant the data point is a member of.
            double difference = Math.abs(currValue) - Math.abs(floor);
            if((difference >= 0.5 && !negative) || (difference < 0.5 && negative)){
                //#HyperOctants increase by 2^n (n = dimensions)
                hyperOctID += Math.pow(2, expCounter);
            }
            expCounter++;
        }
        //Concatenate values so I can use information later
        uniqueID += uniqueIDSize;

        idStorage[0] = Double.parseDouble(uniqueID);
        idStorage[1] = hyperOctID;

        return idStorage;
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