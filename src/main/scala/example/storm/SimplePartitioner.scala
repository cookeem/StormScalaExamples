package example.storm

import kafka.producer.Partitioner
import kafka.utils.VerifiableProperties

/**
 * Created by cookeem on 16/2/26.
 */
class SimplePartitioner(var props: VerifiableProperties) extends Partitioner {
  override def partition(key: scala.Any, numPartitions: Int) = {
    var partition = 0
    partition = Math.abs(key.hashCode()) % numPartitions
    //println(s"###SimplePartitioner: $partition")
    partition
  }
}
