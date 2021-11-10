package day02.source

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.datastream.DataStreamSource
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.kafka.clients.consumer.ConsumerConfig

import java.util.Properties

object Flink04_Source_Kafka {
  def main(args: Array[String]): Unit = {
//    val env: StreamExecutionEnvironment = StreamExecutionEnvironment
//    val properties: Properties = new Properties()
//    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092")
//    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,"flink_test")

//    val kafkaStream: DataStreamSource[String] = env.addSource(new FlinkKafkaConsumer("sensor", new SimpleStringSchema(), properties))
//    kafkaStream.map(record => {
//      record.
//    })
  }
}
