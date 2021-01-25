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


    //result.print()
    // execute program
    env.execute("Flink Streaming Scala API Skeleton")

  }
}


