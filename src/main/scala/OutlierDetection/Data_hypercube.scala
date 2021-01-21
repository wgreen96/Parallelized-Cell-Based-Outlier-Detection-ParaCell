package OutlierDetection

import scala.collection.mutable.ListBuffer

class Data_hypercube(c_val: ListBuffer[Double], c_arrival: Long, c_flag: Int) extends Serializable {

  val value: ListBuffer[Double] = c_val
  val dimensions: Int = value.length
  val arrival: Long = c_arrival
  val flag: Int = c_flag
  val state: Seq[ListBuffer[Double]] = Seq(value)
  val hashcode: Int = state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  var hypercubeID: Int = -1
  var partitionID: Int = -1


  def this(point: Data_basis){
    this(point.value, point.arrival, point.flag)
  }

  def setHypercubeID(newHypercubeID: Int): Unit =
    hypercubeID = newHypercubeID

  def setPartitionId(newPartitionID: Int): Unit =
    partitionID = newPartitionID

override def toString = s"Data_hypercube($value, $arrival, $flag, $hypercubeID, $partitionID)"
//override def toString = s"Data_hypercube($partitionID)"
}



