import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.IntegerSerializer
import org.apache.kafka.common.serialization.StringSerializer
import java.util.Properties

fun main(args: Array<String>) {
    val producer = createProducer()
    val states = listOf("TS", "AP", "UP", "Delhi")
    val districts = listOf("TS Dist", "AP Dist", "UP Dist", "Delhi Dist")
    states.forEachIndexed { index, state ->
        val future = producer.send(ProducerRecord("de-state-topic-2", index, state))
        println(future.get().timestamp())
    }
    districts.forEachIndexed { index, district ->
        val future =producer.send(ProducerRecord("de-district-topic-2", index, district))
        println(future.get().timestamp())
    }
}

private fun createProducer(): Producer<Int, String> {
    val props = Properties()
    props["bootstrap.servers"] = "localhost:9092"
    props["key.serializer"] = IntegerSerializer::class.java
    props["value.serializer"] = StringSerializer::class.java
    return KafkaProducer(props)
}