package pl.roszczyna.movies.topology;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Data
@ConfigurationProperties("kafka.topics")
public class KafkaTopics {
    private String moviesTopic;
    private String ratingsTopic;
    private String outputTopic;
}
