package drugBankTD

import com.twitter.bijection.avro.GenericAvroCodecs
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords, KafkaConsumer}

import java.util
import scala.collection.JavaConverters._


object ConsumerAVRO extends App {
  import java.util.Properties

  val TOPIC="tp1"

  val  props = new Properties()
  props.put("bootstrap.servers", "localhost:9092")

  props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer")
  props.put("group.id", "something")

  val consumer = new KafkaConsumer[String, Array[Byte]](props)

  consumer.subscribe(util.Arrays.asList(TOPIC))

  while(true){
    val records : ConsumerRecords[String, Array[Byte]] = consumer.poll(100);
    records.forEach(r => {

      println("offset = %d, key = %s, value = %s \n", r.offset(), r.key(), r.value())
    })
  }


}
