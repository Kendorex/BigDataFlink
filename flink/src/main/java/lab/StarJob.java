package lab;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class StarJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 5000));

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"))
                .setTopics(System.getenv().getOrDefault("KAFKA_TOPIC", "sales"))
                .setGroupId("flink-star-lab3")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setProperty("enable.auto.commit", "false")
                .build();

        env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka JSON sales")
                .uid("kafka-sales")
                .map(new SaleMapper()).name("JSON to star sale").uid("star-mapping")
                .addSink(new PostgresSink()).name("Transactional PostgreSQL star").uid("postgres-star");

        env.execute("BigDataFlink - Streaming star schema");
    }
}
