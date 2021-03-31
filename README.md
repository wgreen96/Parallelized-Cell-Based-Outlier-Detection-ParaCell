# Parallelized-Cell-Based-Outlier-Detection-ParaCell

ParaCell is an outlier detection algorithm for continous-valued data stream. It utilizes a grid-based outlier detection algorithm for efficient pruning of potential outliers
and passes the remaining data points to LSH for an approximate result. The algorithm was developed using the Apache Flink framework to handle parralel workflows of data streams.


# How to run it

The work was developed in Intellij Idea for ease of testing with Apache Flink. To run the program: clone the repsitory and import it in Intellij Idea as a maven project.

The parameters for running the program are currently hardcoded. They can be found at src/main/scala/OutlierDetection/StreamingJob. The primary parameters are the dataset,
window size, slide size, radius, and minimum points.


# Future work

This algorithm was developed for my thesis and its primary utility was running experiments. Future work will cover building an external JAR that can be run on a local
Apache Flink cluster. There is additional work required for developing a more robust parallel outlier detection algorithm and potentially moving towards a distributed 
environment.
