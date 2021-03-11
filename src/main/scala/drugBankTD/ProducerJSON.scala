package drugBankTD

class ProducerJSON(val seq_records : Seq[String]) {

  import java.util.Properties

  import org.apache.kafka.clients.producer._

  def startProducer(): Unit ={
    val  props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")

    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)

    val TOPIC="test"

    seq_records.foreach(s => {
      val record = new ProducerRecord(TOPIC, "jsonLubm", s)
      producer.send(record)
    })

  }


}