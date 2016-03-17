package example.storm

import java.util.{Properties, UUID}

import backtype.storm.{StormSubmitter, LocalCluster, Config}
import backtype.storm.spout.SchemeAsMultiScheme
import backtype.storm.topology.{TopologyBuilder, OutputFieldsDeclarer, BasicOutputCollector}
import backtype.storm.topology.base.BaseBasicBolt
import backtype.storm.tuple.{Fields, Values, Tuple}
import example.storm.WordCountTest._
import storm.kafka.bolt.KafkaBolt
import storm.kafka.bolt.mapper.FieldNameBasedTupleToKafkaMapper
import storm.kafka.bolt.selector.DefaultTopicSelector
import storm.kafka.{KafkaSpout, StringScheme, SpoutConfig, ZkHosts}

/**
 * Created by cookeem on 16/2/26.
 */
object KafkaStormTest extends App {
  val prompt = "Usage: example.storm.WordCountTest <topicFrom> <topicTo> [toCluster]"
  if (args.length < 2) {
    println(prompt)
  } else {
    val topicFrom = args(0)
    val topicTo = args(1)

    //以下代码表示从kafka的topic02主题consume数据
    //storm的kafka spout相当于kafka的consumer
    val hosts = new ZkHosts("localhost:2181")
    val zkRoot = "/" + topicFrom
    val spoutId = UUID.randomUUID().toString()
    val spoutConfig = new SpoutConfig(hosts, topicFrom, zkRoot, spoutId)
    spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme())
    val kafkaSpout = new KafkaSpout(spoutConfig)

    //以下代码表示向kafka的topic05主题produce数据
    //storm的kafka bolt相当于kafka的producer
    val kafkaBolt = new KafkaBolt()
    .withTopicSelector(new DefaultTopicSelector(topicTo))
    //非常注意: Tuple的Fields必须是key-value对,也就是必须有两个字段,这里用于指定key和message字段的名称
    .withTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper("key", "message"))
    val props = new Properties()
    props.put("metadata.broker.list", "localhost:9092,localhost:9093,localhost:9094")
    props.put("serializer.class", "kafka.serializer.StringEncoder")
    //指定partitioner的类
    props.put("partitioner.class", "example.storm.SimplePartitioner")
    props.put("request.required.acks", "1")

    val conf = new Config()
    conf.setDebug(false)
    //必须设置对应KafkaBolt.KAFKA_BROKER_PROPERTIES
    conf.put(KafkaBolt.KAFKA_BROKER_PROPERTIES, props)

    val builder = new TopologyBuilder()
    //kafkaSpout并行度只能设置为1,否则会出现NodeExists的异常
    builder.setSpout("kafkaSpout", kafkaSpout, 1)
    builder.setBolt("upperBolt", new UpperBolt(), 4).shuffleGrouping("kafkaSpout")
    //kafkaBolt并行度只能设置为1,否则会出现NodeExists的异常
    builder.setBolt("kafkaBolt", kafkaBolt, 1).shuffleGrouping("upperBolt")

    if (args.length > 2) {
      //提交到storm集群上,并且结果不进行输出
      StormSubmitter.submitTopology("kafkaStorm", conf, builder.createTopology())
    } else {
      //以本地模式提交到本地集群,结果输出到客户端,测试用
      val cluster = new LocalCluster()
      cluster.submitTopology("kafkaStorm", conf, builder.createTopology())
    }
  }


  class UpperBolt extends BaseBasicBolt {
    def execute(tuple: Tuple, collector: BasicOutputCollector) {
      val input = tuple.getString(0)
      val output = input.toUpperCase
      println(s"### SplitBolt collector.emit(new Values($output))")
      //非常注意: Tuple的Fields必须是key-value对,也就是必须有两个字段,才能正常输出到KafkaBolt,类似Drpc模式
      collector.emit(new Values(tuple.getValue(0), output))
    }

    def declareOutputFields(declarer: OutputFieldsDeclarer) {
      declarer.declare(new Fields("key", "message"))
    }
  }
}
