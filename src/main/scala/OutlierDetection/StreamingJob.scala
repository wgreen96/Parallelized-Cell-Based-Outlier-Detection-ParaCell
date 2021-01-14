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

import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.datastream.DataStreamUtils
import org.apache.flink.streaming.api.scala._

import scala.collection.JavaConverters.asScalaIteratorConverter
import scala.collection.mutable.ListBuffer

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
    val myInput = "/home/green/Documents/PROUD/data/TAO/input_20k.txt"
    // set up the streaming execution environment
    //val env = StreamExecutionEnvironment.getExecutionEnvironment
    val env = StreamExecutionEnvironment.createLocalEnvironment()

    env.setParallelism(partitions)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val data: DataStream[Data_basis] = {
      println(myInput)
      env
        .readTextFile(myInput)
        .map { record =>
          val splitLine = record.split(line_delimiter)
          val id = splitLine(0).toInt
          val value = splitLine(1).split(delimiter).map(_.toDouble).to[ListBuffer]
          val timestamp = id.toLong
          new Data_basis(id, value, timestamp, 0)
        }
    }

    val myOutput: Iterator[Data_basis] = DataStreamUtils.collect(data.javaStream).asScala
    myOutput.foreach{
      println
    }

    data.writeAsText("/home/green/Documents/testOutputApacheFlink.txt", WriteMode.OVERWRITE)
    //data.print()


    // execute program
    env.execute("Flink Streaming Scala API Skeleton")
  }
}
