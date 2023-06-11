package pl.roszczyna.movies.topology;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.Stores;
import pl.roszczyna.movies.model.Movie;
import pl.roszczyna.movies.serde.SerdeProvider;
import pl.roszczyna.movies.topology.processor.MoviesProcessor;
import pl.roszczyna.movies.topology.processor.RatingsProcessor;

public class MovieCensorshipTopology {

    private KafkaStores kafkaStores;
    private KafkaTopics kafkaTopics;
    private SerdeProvider serdeProvider;

    public MovieCensorshipTopology(KafkaStores kafkaStores, KafkaTopics kafkaTopics, SerdeProvider serdeProvider) {
        this.kafkaStores = kafkaStores;
        this.kafkaTopics = kafkaTopics;
        this.serdeProvider = serdeProvider;
    }

    public KStream<String, Movie> processStream(StreamsBuilder streamsBuilder) {

        streamsBuilder.addStateStore(
                Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore(
                                kafkaStores.getMoviesStore()
                        ),
                        serdeProvider.getMovieKeySerde(),
                        serdeProvider.getMovieSerde()
                )
        );

        streamsBuilder.addStateStore(
                Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore(
                                kafkaStores.getRatingsStore()
                        ),
                        serdeProvider.getMovieKeySerde(),
                        serdeProvider.getMovieRatingSerde()
                )
        );

        var ratedMoviesStreamFromMoviesStream = streamsBuilder.stream(kafkaTopics.getMoviesTopic(),
                        Consumed.with(
                                serdeProvider.getMovieKeySerde(),
                                serdeProvider.getMovieSerde()
                        ).withName("MOVIES_STREAM_CONSUMER"))
                .process(() -> new MoviesProcessor(kafkaStores.getMoviesStore(), kafkaStores.getRatingsStore()),
                        Named.as("RATING_MOVIES_FROM_MOVIES"),
                        kafkaStores.getMoviesStore(), kafkaStores.getRatingsStore());

        var ratedMoviesStreamFromRatingsStream = streamsBuilder.stream(kafkaTopics.getRatingsTopic(),
                        Consumed.with(
                                serdeProvider.getMovieKeySerde(),
                                serdeProvider.getMovieRatingSerde()
                        ).withName("RATINGS_STREAM_CONSUMER"))
                .process(() -> new RatingsProcessor(kafkaStores.getMoviesStore(), kafkaStores.getRatingsStore()),
                        Named.as("RATING_MOVIES_FROM_RATINGS"),
                        kafkaStores.getMoviesStore(), kafkaStores.getRatingsStore());

        var ratedMoviesStream = ratedMoviesStreamFromMoviesStream.merge(ratedMoviesStreamFromRatingsStream, Named.as("RATED_MOVIES_STREAMS_MERGE"));

        var result = ratedMoviesStream
                .filter((key, value) -> value.rating > 50, Named.as("CENSORSHIP_THRESHOLD"));

        result.to(
                kafkaTopics.getOutputTopic(),
                Produced.with(serdeProvider.getMovieKeySerde(),
                        serdeProvider.getMovieSerde())
                        .withName("RATED_MOVIES_PRODUCER")
        );

        return result;

    }
}
