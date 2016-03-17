package example.storm

import backtype.storm.utils.Utils
import backtype.storm.{StormSubmitter, Config, LocalCluster}
import backtype.storm.spout.SpoutOutputCollector
import backtype.storm.task.TopologyContext
import backtype.storm.topology.{TopologyBuilder, BasicOutputCollector, OutputFieldsDeclarer}
import backtype.storm.topology.base.{BaseBasicBolt, BaseRichSpout}
import backtype.storm.tuple.{Tuple, Fields, Values}

import scala.collection.mutable
import scala.util.Random
import java.util.{Map => JMap}

/**
 * Created by cookeem on 16/2/26.
 */
object WordCountTest extends App {
  println("Usage: example.storm.WordCountTest [toCluster]")
  val builder = new TopologyBuilder()
  builder.setSpout("sentenceSpout", new SentenceSpout(), 3)
  builder.setBolt("splitBolt", new SplitBolt(), 4).shuffleGrouping("sentenceSpout")
  builder.setBolt("countBolt", new CountBolt(), 3).fieldsGrouping("splitBolt", new Fields("word"))
  //并行度设置为1,以保证最终汇总到一个节点进行排序
  builder.setBolt("maxBolt", new MaxBolt(), 1).fieldsGrouping("countBolt", new Fields("word"))
  val conf = new Config()
  conf.setDebug(false)
  if (args.length == 1) {
    //提交到storm集群上,并且结果不进行输出
    StormSubmitter.submitTopology("wordCountMax", conf, builder.createTopology())
  } else {
    //以本地模式提交到本地集群,结果输出到客户端,测试用
    val cluster = new LocalCluster()
    cluster.submitTopology("wordCountMax", conf, builder.createTopology())
  }



  //自动生成句子Spout
  class SentenceSpout extends BaseRichSpout {
    var collector: SpoutOutputCollector = _
    val sentences = Seq(
      "Bolts can do simple stream transformations. Doing complex stream transformations often requires multiple steps and thus multiple bolts. For example, transforming a stream of tweets into a stream of trending images requires at least two steps: a bolt to do a rolling count of retweets for each image, and one or more bolts to stream out the top X images (you can do this particular stream transformation in a more scalable way with three bolts than with two).",
      "Bolts can emit more than one stream. To do so, declare multiple streams using the declareStream method of OutputFieldsDeclarer and specify the stream to emit to when using the emit method on OutputCollector.",
      "When you declare a bolt's input streams, you always subscribe to specific streams of another component. If you want to subscribe to all the streams of another component, you have to subscribe to each one individually.",
      "The main method in bolts is the execute method which takes in as input a new tuple. Bolts emit new tuples using the OutputCollector object. Bolts must call the ack method on the OutputCollector for every tuple they process so that Storm knows when tuples are completed (and can eventually determine that its safe to ack the original spout tuples). For the common case of processing an input tuple, emitting 0 or more tuples based on that tuple, and then acking the input tuple, Storm provides an IBasicBolt interface which does the acking automatically."
    )

    def open(conf: JMap[_, _], context: TopologyContext, collector: SpoutOutputCollector) = {
      this.collector = collector
    }

    def nextTuple() = {
      Utils.sleep(5000)
      val randId = Random.nextInt(sentences.length)
      val sentence = sentences(randId)
      println("#########################################")
      collector.emit(new Values(sentence))
    }

    def declareOutputFields(declarer: OutputFieldsDeclarer) = {
      declarer.declare(new Fields("sentence"))
    }
  }

  //分词Bolt
  class SplitBolt extends BaseBasicBolt {
    def execute(tuple: Tuple, collector: BasicOutputCollector) {
      val input = tuple.getStringByField("sentence")
      val words = input.toLowerCase.split("\\W+")
      words.foreach(word => {
        //println(s"### SplitBolt collector.emit(new Values($word))")
        collector.emit(new Values(word))
      })
    }

    def declareOutputFields(declarer: OutputFieldsDeclarer) {
      declarer.declare(new Fields("word"))
    }
  }

  //计算单词数Bolt
  class CountBolt extends BaseBasicBolt {
    var counts = mutable.HashMap[String, Int]()
    def execute(tuple: Tuple, collector: BasicOutputCollector) {
      val word = tuple.getStringByField("word")
      var count = counts.get(word).getOrElse(0)
      count = count + 1
      counts(word) = count
      //println(s"### CountBolt collector.emit(new Values($word, $count))")
      //由于java的不定参数转换到scala异常,必须把value变成AnyRef,否则会抛出异常:
      //the result type of an implicit conversion must be more specific than AnyRef
      collector.emit(new Values(word.asInstanceOf[AnyRef], count.asInstanceOf[AnyRef]))
    }

    def declareOutputFields(declarer: OutputFieldsDeclarer) {
      declarer.declare(new Fields("word", "count"))
    }
  }

  //单词次数排序,输出次数最大的单词前1
  class MaxBolt extends BaseBasicBolt {
    var counts = mutable.HashMap[String, Int]()
    var countsPre = mutable.HashMap[String, Int]()
    def execute(tuple: Tuple, collector: BasicOutputCollector) {
      val word = tuple.getStringByField("word")
      val count = tuple.getIntegerByField("count")
      counts(word) = count
      val maxcounts = counts.toSeq.sortBy(t => -(t._2)).take(1)
      maxcounts.foreach{case (word,count) => {
        //假如数据有增长,才进行显示
        if (count > countsPre.get(word).getOrElse(0)) {
          println(s"### MaxBolt collector.emit(new Values($word, $count))")
          //由于java的不定参数转换到scala异常,必须把value变成AnyRef,否则会抛出异常:
          //the result type of an implicit conversion must be more specific than AnyRef
          //可以把数据直接封装在一个tuple中
          collector.emit(new Values((word, count)))
        }
      }}
      countsPre(word) = count
    }

    def declareOutputFields(declarer: OutputFieldsDeclarer) {
      declarer.declare(new Fields("wordcount"))
    }
  }

}
