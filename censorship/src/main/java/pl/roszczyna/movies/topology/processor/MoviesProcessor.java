package pl.roszczyna.movies.topology.processor;

import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import pl.roszczyna.movies.model.Movie;
import pl.roszczyna.movies.model.Rating;

public class MoviesProcessor implements Processor<String, Movie, String, Movie> {

    private final String moviesStoreName;
    private final String ratingsStoreName;

    private KeyValueStore<String, Movie> moviesStore;
    private KeyValueStore<String, Rating> ratingsStore;

    private ProcessorContext<String, Movie> context;

    public MoviesProcessor(String moviesStoreName, String ratingsStoreName) {
        this.moviesStoreName = moviesStoreName;
        this.ratingsStoreName = ratingsStoreName;
    }

    @Override
    public void init(ProcessorContext<String, Movie> context) {
        Processor.super.init(context);
        this.context = context;
        this.moviesStore = context.getStateStore(moviesStoreName);
        this.ratingsStore = context.getStateStore(ratingsStoreName);
    }

    @Override
    public void process(Record<String, Movie> record) {
        if(record.key() == null)
            return;

        // Processing movie record
        var movieId = record.key();
        var movie = record.value();

        var rating = ratingsStore.get(movieId);
        // No rating yet, save the movie for later
        if(rating == null) {
            moviesStore.put(movieId, movie);
            return;
        }

        // there is a rating for the movie - enrich and forward, cleanup the stores
        if(rating != null) {
            movie.setRating(rating.getScore());
            context.forward(new Record<>(
                    movieId,
                    movie,
                    record.timestamp(),
                    record.headers()
            ));
            ratingsStore.delete(movieId);
            moviesStore.delete(movieId);
        }

    }
}
