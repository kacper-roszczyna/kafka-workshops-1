package pl.roszczyna.movies.serde;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.subject.TopicRecordNameStrategy;
import io.confluent.kafka.streams.serdes.avro.ReflectionAvroSerde;
import lombok.Getter;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import pl.roszczyna.movies.model.Movie;
import pl.roszczyna.movies.model.Rating;

import java.util.Map;

public class SerdeProvider {

    private final SchemaRegistryProperties schemaRegistryProperties;
    @Getter
    private final Serde<Movie> movieSerde;

    @Getter
    private final Serde<String> movieKeySerde = Serdes.String();

    @Getter
    private final Serde<Rating> movieRatingSerde;

    public SerdeProvider(SchemaRegistryProperties schemaRegistryProperties) {
        this.schemaRegistryProperties = schemaRegistryProperties;
        this.movieSerde = createTypedSerde(Movie.class, false);
        this.movieRatingSerde = createTypedSerde(Rating.class, false);
    }

    private String getCredentials() {
        return String.format("%s:%s", schemaRegistryProperties.getUser(), schemaRegistryProperties.getPassword());
    }

    public Map<String, ?> getAvroConfig() {
        return Map.ofEntries(
                Map.entry(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryProperties.getUrl()),
                Map.entry(AbstractKafkaSchemaSerDeConfig.KEY_SUBJECT_NAME_STRATEGY, TopicRecordNameStrategy.class),
                Map.entry(AbstractKafkaSchemaSerDeConfig.VALUE_SUBJECT_NAME_STRATEGY, TopicRecordNameStrategy.class),
                Map.entry(AbstractKafkaSchemaSerDeConfig.USER_INFO_CONFIG, getCredentials())
        );
    }

    public <T> Serde<T> createTypedSerde(Class<T> type, boolean isKey) {
        var serde = new ReflectionAvroSerde<>(type);
        serde.configure(getAvroConfig(), isKey);
        return serde;
    }

}
