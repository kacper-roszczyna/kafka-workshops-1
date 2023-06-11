package pl.roszczyna.movies.topology;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Data
@ConfigurationProperties("kafka.stores")
public class KafkaStores {

    private String moviesStore;
    private String ratingsStore;
}
