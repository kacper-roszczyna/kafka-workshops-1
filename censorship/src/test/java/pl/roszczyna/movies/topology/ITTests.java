package pl.roszczyna.movies.topology;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serializer;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.testcontainers.containers.Network;
import org.testcontainers.shaded.org.apache.commons.io.FileUtils;
import pl.roszczyna.movies.MovieCensorshipApplication;
import pl.roszczyna.movies.container.KafkaTestContainer;
import pl.roszczyna.movies.container.SchemaRegistryContainer;
import pl.roszczyna.movies.model.Movie;
import pl.roszczyna.movies.model.Rating;
import pl.roszczyna.movies.serde.SerdeProvider;
import pl.roszczyna.movies.utils.TestConsumer;

import java.io.File;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Slf4j
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
@ActiveProfiles({"container"})
@ExtendWith(SpringExtension.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@SpringBootTest(classes = {MovieCensorshipApplication.class})
public class ITTests {

    static Network network = Network.newNetwork();
    static KafkaTestContainer kafkaContainerWrapper = KafkaTestContainer.getContainer(network)
            .registerTopics(Arrays.asList(
                    "MOVIES",
                    "RATINGS",
                    "RATED_MOVIES",
                    "KACPER_CONSUMER_GROUP_ID-MOVIES-STORE-changelog",
                    "KACPER_CONSUMER_GROUP_ID-RATINGS-STORE-changelog"
            ));

    static SchemaRegistryContainer schemaRegistryContainer = SchemaRegistryContainer.get(network, kafkaContainerWrapper.getNetworkAliases().get(0));

    @DynamicPropertySource
    static void registerContainerProps(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", kafkaContainerWrapper::getBootstrapServers);
        registry.add("schema.registry.url", schemaRegistryContainer::getSchemaRegistryUrl);
    }

    @Autowired
    private KafkaTopics kafkaTopics;

    @Autowired
    private KafkaStores kafkaStores;

    @Autowired
    private StreamsBuilderFactoryBean streamsBuilderFactoryBean;

    @Value("${spring.kafka.properties.state.dir}" )
    private String stateDir;

    @Autowired
    private SerdeProvider serdeProvider;
    private Producer<String, Movie> moviesProducer;
    private Producer<String, Rating> ratingsProducer;

    @BeforeEach
    public void setUp() {
        moviesProducer = getProducer(serdeProvider.getMovieKeySerde().serializer(), serdeProvider.getMovieSerde().serializer());
        ratingsProducer = getProducer(serdeProvider.getMovieKeySerde().serializer(), serdeProvider.getMovieRatingSerde().serializer());
    }

    @AfterEach
    @SneakyThrows
    public void tearDown() {
        moviesProducer.close();
        ratingsProducer.close();
        Objects.requireNonNull(streamsBuilderFactoryBean.getKafkaStreams()).close(Duration.ofSeconds(15));
        FileUtils.deleteDirectory(new File(stateDir));
        kafkaContainerWrapper.resetTopics();
    }

    @Test
    @SneakyThrows
    public void testRatings() {
        // Create simple producers
        var moviesProducer = getProducer(serdeProvider.getMovieKeySerde().serializer(), serdeProvider.getMovieSerde().serializer());
        var ratingsProducer = getProducer(serdeProvider.getMovieKeySerde().serializer(), serdeProvider.getMovieRatingSerde().serializer());

        // Produce a couple of messages
        // Movies
        moviesProducer.send(new ProducerRecord<>(kafkaTopics.getMoviesTopic(), "A-ClassMovie", new Movie("Best Movie Ever Made", null))).get();
        moviesProducer.send(new ProducerRecord<>(kafkaTopics.getMoviesTopic(), "B-ClassMovie", new Movie("Your Average Action Movie", null))).get();
        moviesProducer.send(new ProducerRecord<>(kafkaTopics.getMoviesTopic(), "Z-ClassMovie", new Movie("Complete Trash", null))).get();

        // Now check the schema registry if you've registered a schema
        // http://localhost:32793/subjects/MOVIES-value/versions/1 - change the port to what has been set
        // Ratings
        ratingsProducer.send(new ProducerRecord<>(kafkaTopics.getRatingsTopic(), "A-ClassMovie", new Rating("A-ClassMovie", 100)));
        ratingsProducer.send(new ProducerRecord<>(kafkaTopics.getRatingsTopic(), "B-ClassMovie", new Rating("B-ClassMovie", 65)));
        ratingsProducer.send(new ProducerRecord<>(kafkaTopics.getRatingsTopic(), "Z-ClassMovie", new Rating("Z-ClassMovie", 12)));

        var outputConsumer = new TestConsumer<String, Movie>(
                kafkaContainerWrapper.getBootstrapServers(),
                serdeProvider.getMovieKeySerde().deserializer(),
                serdeProvider.getMovieSerde().deserializer(),
                kafkaTopics.getOutputTopic(),
                schemaRegistryContainer.getSchemaRegistryUrl()
        );

        outputConsumer.subscribe(kafkaTopics.getOutputTopic());

        Awaitility.await().atMost(Duration.ofMinutes(1)).untilAsserted(() -> {
            var output = outputConsumer.consume();
            var results = output.stream()
                            .collect(Collectors.groupingBy(ConsumerRecord::key));
            assertEquals(2, output.size());
            assertEquals(results.get("A-ClassMovie").get(0).value(), new Movie("Best Movie Ever Made", 100));
            assertEquals(results.get("B-ClassMovie").get(0).value(), new Movie("Your Average Action Movie", 65));
        });

        outputConsumer.close();
    }

    public <K, V> Producer<K, V>  getProducer(Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        Map<String, Object> producerConfigs = new HashMap<>(
                        KafkaTestUtils.producerProps(kafkaContainerWrapper.getBootstrapServers())
        );
        producerConfigs.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryContainer.getSchemaRegistryUrl());
        return new DefaultKafkaProducerFactory<>(producerConfigs, keySerializer, valueSerializer).createProducer();
    }

}
