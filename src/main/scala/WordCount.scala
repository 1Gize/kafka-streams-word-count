import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.{KafkaStreams, StreamsBuilder, StreamsConfig}
import org.apache.kafka.streams.kstream.{KStream, Printed}

import java.util
import java.util.Properties

object WordCount extends App{
  val ksb = new StreamsBuilder
  val input: KStream[String, String] = ksb.stream("ksWordCount")
  val wordCount = input.
    flatMapValues(line =>  util.Arrays.asList(line.split("\\W+")))
    .groupBy((key,w) => w)
    .count()
  input.print(Printed.toSysOut[String,String].withLabel("[What]"))
  val topology = ksb.build()
  println(topology.describe())
  val props = new Properties()
  props.put(StreamsConfig.APPLICATION_ID_CONFIG,"ks-word-counter")
  props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,":9092")
  props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass)
  props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass)
  private val ks = new KafkaStreams(topology,props)
  ks.start()
}
