package OutlierDetection;


import java.util.ArrayList;

public class HypercubeGeneration {

    static double dimensions;
    static double hypercubeSide;
    static int partitions;


    public void HypercubeGeneration(){

    }

    public static Hypercube createPartitions(Hypercube dataPoint){
        String hypercubeID = "";
        //Create data structure to store values that will become HypercubeID
        ArrayList<Double> arrayOfMeans = new ArrayList<>();
        double[] arrayOfIDs = new double[(int)dimensions];
        //For each coordinate
        for(Double val : dataPoint.coords) {
            //Find closest multiple of hypercubeSide. 0.0001 is included to stop really small values from being concatenated to 0.0
            double closestMultiple = val / hypercubeSide + 0.00000001;
            //Ceiling and floor are to ensure any values in range of those 2 end up with same hypercubeID
            int ceiling = (int) Math.ceil(closestMultiple);
            int floor = (int) Math.floor(closestMultiple);
            //This creates a unique id for the same number, positive or negative. (ex = 4.1) 4.1 = 54, -4.1 = 45
            hypercubeID += Integer.toString(Math.abs(ceiling));
            hypercubeID += Integer.toString(Math.abs(floor));
            double meanValue = (double)(ceiling + floor) / 2;
            arrayOfMeans.add(meanValue);
        }
        int newPartitionID = (int) (Math.abs(Double.parseDouble(hypercubeID)) % partitions);
        return new Hypercube(dataPoint.coords, dataPoint.arrival, hypercubeID, newPartitionID, arrayOfMeans);
    }

    public static double[] createIDs(ArrayList<Double> multiplicationVals){
        String uniqueID = "";
        double[] idStorage = new double[2+(int)dimensions];
        int index = 1;

        //Concatenate int values
        //So {1.3,4.7,63.1} creates uniqueID 21546463
        for(double currValue : multiplicationVals){
            //Ceiling and floor are to ensure any values in range of those 2 end up with same hypercubeID
            int ceiling = (int) Math.ceil(currValue);
            int floor = (int) Math.floor(currValue);
            //This creates uniqueID for the same positive and negative number. (ex = 4.1) 4.1 = 54, -4.1 = 45
            uniqueID += Integer.toString(Math.abs(ceiling));
            uniqueID += Integer.toString(Math.abs(floor));
            double meanValue = (double)(ceiling + floor) / 2;
            idStorage[index] = meanValue;
            index++;
        }

        idStorage[0] = Double.parseDouble(uniqueID);

        return idStorage;
    }
}