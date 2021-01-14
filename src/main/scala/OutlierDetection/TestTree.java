package OutlierDetection;

public class TestTree {

    //TODO NEED TO ENSURE EUCLIDEAN SPACE IS HYPERCUBESIDES * POW(DIMENSIONS)

    public static void main(String args[])
    {
        //Hardcoded values, will need to be changed after this is finished
        double radius = 5;
        double diagonal = radius/2;
        int dimensions = 2;
        float hypercubeSide = (float) (diagonal/Math.sqrt(dimensions));
        //TODO THIS 2 WILL BE THE ONLY WAY I CAN EXPAND THE SPACE WHILE WORKING WITH ODD DIMENSIONS. IS IT 2n or 2^n?
        double sizeOfSpaceExpansion = Math.pow(2, dimensions) * 2;
        float euclideanSize = (float) (hypercubeSide * sizeOfSpaceExpansion);

        //Determine size of entire euclidean space
        //The size is divided by 2 because we are working from the point of origin, so the values are (-maxSize to maxSize) instead of euclideanSize
        float maxSize = euclideanSize/2;
        float desiredHypercubeSide = hypercubeSide;

        int numberOfPartitions = Math.round(maxSize/desiredHypercubeSide);
        if(numberOfPartitions % Math.pow(2, dimensions) != 0 || numberOfPartitions < Math.pow(2,dimensions)){
            System.out.println("Cannot create an equal number of partitions. Correct Partitions: " + Math.pow(2, dimensions) + " Current: " + numberOfPartitions);
            System.exit(0);
        }

        //Create controller and tree
        TestTree controller = new TestTree();
        controller.generateTree(maxSize, dimensions, desiredHypercubeSide);

    }


    public void generateTree(float maxSize, int dimensions, float hypercubeSize)
    {
        //Generate coordinate array based off number of dimensions
        float[] coordArray = new float[dimensions];
        for(int zeroIndex = 0; zeroIndex < dimensions; zeroIndex++){
            coordArray[zeroIndex] = 0;
        }
        //Create tree
        TestKDTree currTree = new TestKDTree(dimensions, maxSize, hypercubeSize);
        //Create root node
        currTree.createRoot(coordArray);
        currTree.createPartitions(currTree.root);

        System.out.println("Hello World");
    }


}
