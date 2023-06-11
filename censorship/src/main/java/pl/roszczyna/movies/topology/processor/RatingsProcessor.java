package pl.roszczyna.movies.topology.processor;

import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import pl.roszczyna.movies.model.Movie;
import pl.roszczyna.movies.model.Rating;

public class RatingsProcessor implements Processor<String, Rating, String, Movie> {

    private final String movieStoreName;
    private final String ratingsStoreName;

    private KeyValueStore<String, Movie> moviesStore;
    private KeyValueStore<String, Rating> ratingsStore;
    private ProcessorContext<String, Movie> context;

    public RatingsProcessor(String movieStoreName, String ratingsStoreName) {
        this.movieStoreName = movieStoreName;
        this.ratingsStoreName = ratingsStoreName;
    }

    @Override
    public void init(ProcessorContext<String, Movie> context) {
        Processor.super.init(context);
        this.context = context;
        this.moviesStore = context.getStateStore(movieStoreName);
        this.ratingsStore = context.getStateStore(ratingsStoreName);
    }

    @Override
    public void process(Record<String, Rating> record) {
        if(record.key() == null)
            return;

        var movieId = record.key();
        var rating = record.value();
        var movie = moviesStore.get(movieId);

        // Process a rating record - movie is not present - store for later
        if(movie == null) {
            ratingsStore.put(movieId, rating);
        }

        if(movie != null) {
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
