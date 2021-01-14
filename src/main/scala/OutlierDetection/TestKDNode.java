package OutlierDetection;

import java.io.Serializable;
import java.util.Arrays;

public class TestKDNode implements Serializable {
    TestKDNode left = null;
    TestKDNode right = null;
    TestKDNode parent = null;

    public float[] currAxisValues;
    public float[] currBoundary;
    float sideLength = 0.01f;
    int level;
    float difference = 0.01f;

    public TestKDNode(){

    }

    public TestKDNode(TestKDNode parent, float[] coords, int level){
        this.level = level;
        //Does this ensure a deep copy is made? If that is not true, I could do away with this and save memory
        this.setCoords(coords);
    }

    public void setCoords(float [] coords)
    {
        this.currAxisValues = coords.clone();
    }

    public void setBoundary(float [] bounds){
        this.currBoundary = bounds.clone();
    }


    public float[] currAxisValues(){
        return this.currAxisValues;
    }

    public float[] currBoundary(){
        return this.currBoundary;
    }

    public String toString()
    {
        return Arrays.toString(this.currAxisValues);
    }

    public void free()
    {
        this.currAxisValues = null;
    }

}
