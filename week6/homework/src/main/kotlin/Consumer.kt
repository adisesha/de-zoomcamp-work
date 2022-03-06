import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.kstream.*
import java.time.Duration
import java.util.*


fun main(args: Array<String>) {
    val builder = StreamsBuilder()
    val stateStream: KStream<Int, String> =
        builder.stream("de-state-topic-2", Consumed.with(Serdes.Integer(), Serdes.String()))

    val districtStream: KStream<Int, String> =
        builder.stream("de-district-topic-2", Consumed.with(Serdes.Integer(), Serdes.String()))

    val joiner =
        ValueJoiner<String, String, String> { value1, value2 -> "$value1 -> $value2" }

    val innerJoin = stateStream.join(
        districtStream,
        joiner,
        JoinWindows.of(Duration.ofDays(5)),
        StreamJoined.with(
            Serdes.Integer(),
            Serdes.String(),
            Serdes.String()
        )
    )
    innerJoin.foreach  { key, value ->  println("$key -> $value") }
    val kafkaStreams = KafkaStreams(builder.build(), getProps())
    kafkaStreams.start()
    kafkaStreams.localThreadsMetadata().forEach { data -> println(data) }
    Runtime.getRuntime().addShutdownHook(Thread { kafkaStreams.close() })
}

private fun getProps(): Properties {
    val props = Properties()
    props[StreamsConfig.BOOTSTRAP_SERVERS_CONFIG] = "localhost:9092"
    props[StreamsConfig.APPLICATION_ID_CONFIG] = "de-zoomcamp-join-example"
    props[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = "earliest"
    return props
}