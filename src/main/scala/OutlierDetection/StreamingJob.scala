/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package OutlierDetection

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.api.{FieldExpression, Table}



/**
 * Skeleton for a Flink Streaming Job.
 *
 * For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="http://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
object StreamingJob {
  def main(args: Array[String]) {
    val delimiter = ","
    val line_delimiter = "&"
    val partitions = 1
    val myInput = "C:/Users/wgree//Git/PROUD/data/STK/input_20k.txt"
    val dataset = "STK"
    val common_R = 0.35

    // set up the streaming execution environment
    //val fsSettings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build()
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //val env = StreamExecutionEnvironment.createLocalEnvironment()
    // set up the table environment for TABLE API
    val tableEnv = StreamTableEnvironment.create(env)
    env.setParallelism(partitions)
    //TODO LOOK INTO WHY THIS ID DEPREACTED AND WHAT REPLACED IT
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)


//    val data: DataStream[ListBuffer[Double], Long] = {
//      //println(myInput)
//      env
//        .readTextFile(myInput)
//        .map { record =>
//          val splitLine = record.split(line_delimiter)
//          val id = splitLine(0).toInt
//          val value = splitLine(1).split(delimiter).map(_.toDouble).to[ListBuffer]
//          val timestamp = id.toLong
//          new Collector(value, timestamp)
//          //new Data_hypercube(value, timestamp)
//        }
//    }

    val data: DataStream[HypercubePoint] = {
      //println(myInput)
      env
        .readTextFile(myInput)
        .map { record =>
          val splitLine = record.split(line_delimiter)
          val id = splitLine(0).toInt
          val value = splitLine(1).split(delimiter).map(_.toDouble)
          val timestamp = id.toLong
          new HypercubePoint(value, timestamp)
        }
    }


    val newData = data.map(record => HypercubeGeneration.createPartitions(partitions, record))



    // convert the DataStream into a Table with default fields "_1", "_2"
    val table1: Table = tableEnv.fromDataStream(newData)
    // convert the DataStream into a Table with fields "myLong", "myString"
    //val table2: Table = tableEnv.fromDataStream(newData, $"value", $"dim", $"arrival", $"flag", $"state", $"hashcode", $"hypID", $"parID")
    val table2: Table = tableEnv.fromDataStream(newData, $"coords", $"arrival", $"hypID", $"parID")
    //val result = tableEnv.sqlQuery("select value from " + table2)
    //newData.writeAsText("C:/Users/wgree/Documents/testOutputApacheFlink.txt", WriteMode.OVERWRITE)
    val colNames = table2.getSchema.getFieldNames
    val colType = table2.getSchema.getFieldDataTypes
    //table1.printSchema()
    colNames.foreach { println}
    colType.foreach { println}

    //result.print()
    // execute program
    env.execute("Flink Streaming Scala API Skeleton")

  }
}

//works, can be used later for debugging
//    val myOutput: Iterator[Data_basis] = DataStreamUtils.collect(data.javaStream).asScala
//    myOutput.foreach{
//      println
//    }
