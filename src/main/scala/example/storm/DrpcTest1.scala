package example.storm

import backtype.storm.{LocalCluster, LocalDRPC, Config}
import backtype.storm.drpc.LinearDRPCTopologyBuilder
import backtype.storm.topology.{BasicOutputCollector, OutputFieldsDeclarer}
import backtype.storm.topology.base.BaseBasicBolt
import backtype.storm.tuple.{Fields, Values, Tuple}

import scala.collection.mutable

/**
 * Created by cookeem on 16/2/25.
 */
object DrpcTest1 extends App {
  val builder = new LinearDRPCTopologyBuilder("splitword")
  builder.addBolt(new SplitBolt(), 3)
  builder.addBolt(new CountBolt(), 3).fieldsGrouping(new Fields("word"))
  val conf = new Config()
  conf.setDebug(false)
  val drpc = new LocalDRPC()
  val cluster = new LocalCluster()
  cluster.submitTopology("drpc-demo", conf, builder.createLocalTopology(drpc))
  val inputArr = Array(
    "Tuples can be comprised of objects of any types. Since Storm is a distributed system, it needs to know how to serialize and deserialize objects when they're passed between tasks.",
    "Storm uses Kryo for serialization. Kryo is a flexible and fast serialization library that produces small serializations.",
    "By default, Storm can serialize primitive types, strings, byte arrays, ArrayList, HashMap, HashSet, and the Clojure collection types. If you want to use another type in your tuples, you'll need to register a custom serializer."
  )
  inputArr.foreach(sentence => {
    val response = drpc.execute("splitword", sentence)
    println(s"### DRPC response: $response")
  })


  //BaseBasicBolt无需自己编写ack以及fail
  //BaseRichBolt则需要自己编写ack以及fail
  class SplitBolt extends BaseBasicBolt {
    def execute(tuple: Tuple, collector: BasicOutputCollector) {
      val input = tuple.getString(1)
      val words = input.toLowerCase.split("\\W+")
      words.foreach(word => {
        println(s"### SplitBolt collector.emit(new Values(${tuple.getValue(0)}, $word))")
        //注意DRPC只是支持两个字段的Tuple[Int, Any]
        collector.emit(new Values(tuple.getValue(0), word))
      })
    }

    def declareOutputFields(declarer: OutputFieldsDeclarer) {
      declarer.declare(new Fields("id", "word"))
    }
  }

  class CountBolt extends BaseBasicBolt {
    var counts = mutable.HashMap[String, Int]()
    def execute(tuple: Tuple, collector: BasicOutputCollector) {
      val word = tuple.getStringByField("word")
      var count = counts.get(word).getOrElse(0)
      count = count + 1
      counts(word) = count
      println(s"### CountBolt collector.emit(new Values(${tuple.getValue(0)}, ($word, $count)))")
      //注意DRPC只是支持两个字段的Tuple[Int, Any]
      collector.emit(new Values(tuple.getValue(0), (word, count)))
    }

    def declareOutputFields(declarer: OutputFieldsDeclarer) {
      declarer.declare(new Fields("id", "wordcount"))
    }
  }
}
