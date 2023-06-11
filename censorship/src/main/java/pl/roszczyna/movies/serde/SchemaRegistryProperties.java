package pl.roszczyna.movies.serde;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Data
@ConfigurationProperties("schema.registry")
public class SchemaRegistryProperties {
    private String url;
    private String user;
    private String password;
}
