package OutlierDetection

import scala.collection.mutable.ListBuffer

class Data_hypercube(c_val: ListBuffer[Double], c_arrival: Long) extends Serializable {

  val value: ListBuffer[Double] = c_val
  val arrival: Long = c_arrival
  var hypercubeID: Int = -1
  var partitionID: Int = -1


  def this(point: Data_hypercube){
    this(point.value, point.arrival)
  }

  def setHypercubeID(newHypercubeID: Int): Unit =
    hypercubeID = newHypercubeID

  def setPartitionId(newPartitionID: Int): Unit =
    partitionID = newPartitionID

  override def toString = s"Data_hypercube($value, $arrival, $hypercubeID, $partitionID)"

  case class Data_hypercube(value: ListBuffer[Double], arrival: Long, hypercubeID: Int, partitionID: Int)
}



