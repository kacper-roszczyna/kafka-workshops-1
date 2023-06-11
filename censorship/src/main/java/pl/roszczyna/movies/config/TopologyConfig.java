package pl.roszczyna.movies.config;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import pl.roszczyna.movies.model.Movie;
import pl.roszczyna.movies.serde.SchemaRegistryProperties;
import pl.roszczyna.movies.serde.SerdeProvider;
import pl.roszczyna.movies.topology.KafkaStores;
import pl.roszczyna.movies.topology.KafkaTopics;
import pl.roszczyna.movies.topology.MovieCensorshipTopology;

@Configuration
public class TopologyConfig {

    @Bean
    public SerdeProvider serdeProvider(SchemaRegistryProperties schemaRegistryProperties) {
        return new SerdeProvider(schemaRegistryProperties);
    }

    @Bean
    public MovieCensorshipTopology movieCensorshipTopology(KafkaTopics kafkaTopics,
                                                           KafkaStores kafkaStores,
                                                           SerdeProvider serdeProvider) {
        return new MovieCensorshipTopology(kafkaStores, kafkaTopics, serdeProvider);
    }

    @Bean
    public KStream<String, Movie> processTopology(StreamsBuilder streamsBuilder,
                                                  MovieCensorshipTopology topology) {
        return topology.processStream(streamsBuilder);
    }

}
