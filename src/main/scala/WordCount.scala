import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.{KafkaStreams, StreamsBuilder, StreamsConfig, scala}
import org.apache.kafka.streams.kstream.{KStream, Printed}
import org.apache.kafka.streams.scala._

import java.util
import java.util.Properties
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.serialization.Serdes._
object WordCount extends App{
  val ksb = new scala.StreamsBuilder()
  val input = ksb.stream[String,String]("ksWordCount")
     input
    .flatMapValues{_.split("\\W+")}
    .groupBy{(_,w) => w}
    .count
    .mapValues(_.toString)
       .toStream
    .to("output1")
  input.print(Printed.toSysOut[String,String].withLabel("[What]"))
  val topology = ksb.build()
  println(topology.describe())
  val props = new Properties()
  props.put(StreamsConfig.APPLICATION_ID_CONFIG,"ks-word-counter")
  props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,":9092")
  props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass)
  props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass)
  val streamCfg = new StreamsConfig(props)
  val ks = new KafkaStreams(topology,streamCfg)
  ks.start()
}
