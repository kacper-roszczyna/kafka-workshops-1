package pl.roszczyna.movies.utils;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.compress.utils.Lists;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.Deserializer;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@Slf4j
public class TestConsumer<K, V> implements Closeable {

    private final Consumer<K, V> consumer;
    private final List<ConsumerRecord<K, V>> receivedMessages = Lists.newArrayList();

    /**
     * If using this method remember to use the subscribe method otherwise no messages will be received
     */
    public TestConsumer(
            String brokers,
            Deserializer<K> keyDeserializer,
            Deserializer<V> valueDeserializer,
            String topic,
            String schemaRegistryUrl
    ) {
        Map<String, Object> consumerConfigs = KafkaTestUtils.consumerProps(brokers,
                "TEST_CONSUMER_GROUP", "true"
        );
        consumerConfigs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerConfigs.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);

        consumer =
                new DefaultKafkaConsumerFactory<>(consumerConfigs, keyDeserializer, valueDeserializer).createConsumer();

    }

    public void subscribe(String topicName) {
        consumer.subscribe(Collections.singletonList(topicName));
    }

    public List<ConsumerRecord<K, V>> consume() {
        ConsumerRecords<K, V> records = consumer.poll(Duration.ofMillis(250));
        records.forEach(message -> {
            receivedMessages.add(message);
            log.info("Consumed message from topic: {}, with key: {}, with value: {}", message.topic(), message.key(),
                    message.value());
        });
        return receivedMessages;
    }

    public void clear() {
        receivedMessages.clear();
    }

    @Override
    public void close() throws IOException {
        consumer.unsubscribe();
        consumer.close(Duration.ofSeconds(1));
    }
}
