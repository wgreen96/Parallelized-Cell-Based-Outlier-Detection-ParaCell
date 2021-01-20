package OutlierDetection;

import java.util.ArrayList;

public class TestTree {

    public static void main(String args[])
    {
        //Hardcoded values, will need to be changed after this is finished
        double radius = 5;
        double diagonal = radius/2;
        int dimensions = 3;
        float hypercubeSide = (float) (diagonal/Math.sqrt(dimensions));
        //We can only have a number of partitions equal to the number of leaf nodes and leaf nodes can only be equal to 2^n.
        int expansion = dimensions + 5;
        double sizeOfSpaceExpansion = Math.pow(2, expansion);
        //The value returned by sizeOfSpaceExpansion ensures the euclidean space has sizeOfSpaceExpansion * 1/2 equally sized hypercubes
        float euclideanSize = (float) (hypercubeSide * sizeOfSpaceExpansion);

        //Determine size of entire euclidean space
        //The size is divided by 2 because we are working from the point of origin, so the values are (-maxSize to maxSize) instead of euclideanSize
        float maxSize = euclideanSize/2;
        System.out.println("Size of euclidean space: " + maxSize);
        float desiredHypercubeSide = hypercubeSide;

        int numberOfPartitions = Math.round(maxSize/desiredHypercubeSide);
        System.out.println("Num partition: " + numberOfPartitions);
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
        //System.out.println(currTree.funCounter);
        ArrayList<TestKDNode> storage = currTree.collectLeafNodes(currTree.root);
        System.out.println(currTree.sameCounter);
        System.out.println(storage.size());
    }


}
