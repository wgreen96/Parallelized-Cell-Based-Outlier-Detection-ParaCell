package OutlierDetection;

public class TestKDTree {

    public TestKDNode root;
    float maxSpaceSize;
    float atomicSize;
    int dim;
    float tolerance = 0.0001f;

    public TestKDTree(int dimension, float maxSize, float atomicSize){
        this.dim = dimension;
        this.maxSpaceSize = maxSize;
        this.atomicSize = atomicSize;
        this.root = null;
    }


    public void createRoot(float[] coords)
    {
        if(this.root == null){
            this.root = new TestKDNode(null, coords, 0);

        }else
        {
            System.out.println("ERROR: The root node has already been generated");
            System.exit(0);
        }
    }


    public void createPartitions(TestKDNode curNode)
    {
        //Use modulo to iterate through each dimension
        int currDim = curNode.level%(this.dim);

        //If all dimensions have been traversed, or it is the first partition, calculate the hypercube's side
        if(currDim == 0){
            //Generate the size of the current hypercube
            calculateHypercubeSide(curNode);
            curNode.difference = Math.abs(curNode.sideLength - atomicSize);
        }else{
            curNode.difference = tolerance + 1;
        }

        //We are comparing floats, so a tolerance ensures tiny deviations shouldn't break everything
        if(tolerance < curNode.difference){
            //Increment the level so the recursive call uses the next dimension
            int nextLevel = curNode.level + 1;
            //Calculate formula for dimensions
            Object[] storedCoords = calculateNewCoords(currDim, curNode);
            curNode.left = new TestKDNode(curNode, (float[]) storedCoords[0], nextLevel);
            createPartitions(curNode.left);
            curNode.right = new TestKDNode(curNode, (float[]) storedCoords[1], nextLevel);
            createPartitions(curNode.right);
        }
    }


    public Object[] calculateNewCoords(int currDim, TestKDNode thisNode){
        float[] currCoords = thisNode.currAxisValues();
        float maxVal = this.maxSpaceSize;
        //The + 1 is necessary because the maxVal is half the actual size of the original hypercube
        int currentIteration = (thisNode.level / this.dim) + 1;
        //For each iteration, the coordinates will move by half of the value from the previous iteration
        float subtractValue = (float) Math.abs(maxVal/Math.pow(2, currentIteration));

        if(currCoords[currDim] == 0f){
            float[] newLeftCoords = currCoords.clone();
            float[] newRightCoords = currCoords.clone();
            float newCoord = maxVal/2;
            newLeftCoords[currDim] = -newCoord;
            newRightCoords[currDim] = newCoord;
            return new Object[]{newLeftCoords, newRightCoords};
        }else{
            float[] newLeftCoords = currCoords.clone();
            float[] newRightCoords = currCoords.clone();
            System.out.println("Current value: " + currCoords[currDim]);
            System.out.println("Value to subtract from current value: " + Math.abs(currCoords[currDim]/2));
            float newLeftXAxis = currCoords[currDim] - subtractValue;
            float newRightXAxis = currCoords[currDim] + subtractValue;
            newLeftCoords[currDim] = newLeftXAxis;
            newRightCoords[currDim] = newRightXAxis;
            return new Object[]{newLeftCoords, newRightCoords};
        }
    }

    public void calculateHypercubeSide(TestKDNode currNode){
        //This indicates the number of times all dimensions have been looped through
        int currentIteration = currNode.level / this.dim;
        if(currentIteration > 1){
            //The - 1 in (currentIteration - 1) is necessary because the maxSpaceSize is half the actual size of the original hypercube
            currNode.sideLength = (float) (maxSpaceSize / Math.pow(2,(currentIteration - 1)));
            if(currNode.sideLength >= atomicSize){
                //System.out.println("Side length " + currNode.sideLength);
            }else{
                System.out.println("Error: The value of side length is too small");
                System.exit(0);
            }
        }
    }
}
