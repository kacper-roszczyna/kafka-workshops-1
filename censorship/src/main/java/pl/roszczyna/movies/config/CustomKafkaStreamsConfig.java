package pl.roszczyna.movies.config;

import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.config.KafkaStreamsCustomizer;
import org.springframework.kafka.config.KafkaStreamsInfrastructureCustomizer;
import org.springframework.kafka.config.StreamsBuilderFactoryBeanConfigurer;
import org.springframework.kafka.streams.KafkaStreamsMicrometerListener;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.function.Consumer;

@Slf4j
@EnableKafka
@EnableKafkaStreams
@Configuration
@ConfigurationPropertiesScan(basePackages = {"pl.roszczyna.movies.*"})
public class CustomKafkaStreamsConfig {

    public static KafkaStreamsConfiguration getKafkaStreamsConfiguration(
            KafkaProperties kafkaProperties
    ) {
        Map<String, Object> props = kafkaProperties.buildStreamsProperties();
        props.putAll(kafkaProperties.buildProducerProperties());
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, Math.max(1, Runtime.getRuntime().availableProcessors() - 1));
        props.put(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, StreamsConfig.OPTIMIZE);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, JsonSerde.class);
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, JsonSerde.class);
        return new KafkaStreamsConfiguration(props);
    }

    @Bean(KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    public KafkaStreamsConfiguration kafkaStreamsConfiguration(
            KafkaProperties kafkaProperties
    ) {
        return getKafkaStreamsConfiguration(kafkaProperties);
    }

    @Bean
    public KafkaStreamsCustomizer kafkaStreamsCustomizer(
    ) {

        return kafkaStreams -> {

            Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
                @Override
                public void run() {
                    kafkaStreams.close(Duration.of(60, ChronoUnit.SECONDS));
                }
            });
        };
    }

    private KafkaStreamsInfrastructureCustomizer topologyLoggingCustomizer() {
        return new KafkaStreamsInfrastructureCustomizer() {
            @Override
            public void configureBuilder(StreamsBuilder builder) {
            }

            @Override
            public void configureTopology(Topology topology) {
                log.info("[TOPOLOGY-DESCRIBE] {}", topology.describe());
            }
        };
    }

    @Bean
    public StreamsBuilderFactoryBeanConfigurer streamsBuilderFactoryBeanConfigurer(
            KafkaStreamsCustomizer kafkaStreamsCustomizer,
            MeterRegistry meterRegistry
    ) {
        return streamsBuilderFactoryBean -> {
            streamsBuilderFactoryBean.setKafkaStreamsCustomizer(kafkaStreamsCustomizer);
            streamsBuilderFactoryBean.addListener(new KafkaStreamsMicrometerListener(meterRegistry));
            streamsBuilderFactoryBean.setInfrastructureCustomizer(topologyLoggingCustomizer());
            streamsBuilderFactoryBean.setCloseTimeout(60);
        };
    }

    @Bean
    public Consumer<Exception> springApplicationExitHandler(ApplicationContext applicationContext) {
        return ex -> {
            var code = SpringApplication.exit(applicationContext, () -> -1);
            System.exit(code);
        };
    }
}
