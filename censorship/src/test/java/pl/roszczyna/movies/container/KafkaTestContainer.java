package pl.roszczyna.movies.container;

import com.google.common.collect.ImmutableMap;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicListing;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.shaded.org.awaitility.Awaitility;
import org.testcontainers.utility.DockerImageName;

import java.util.*;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Slf4j
public class KafkaTestContainer extends KafkaContainer {

    private static KafkaTestContainer CONTAINER;
    private static AdminClient adminClient;
    private static final Set<String> empty = new HashSet<>();

    private List<String> topicsRegistered = new ArrayList<>();


    private KafkaTestContainer(String version) {
        super(DockerImageName.parse("confluentinc/cp-kafka:7.0.1"));
    }

    // add the schema registry to the demo + see the schema registry entry in api
    // simplify the demo to only have ratings
    // add drawings to the ecosystem presentation
    public static KafkaTestContainer getContainer(Network network) {
        if (CONTAINER == null) {
            log.info("[TEST_CONTAINERS] INSTANTIATED KAFKA!");
            CONTAINER = new KafkaTestContainer("confluentinc/cp-kafka:7.0.1");
            CONTAINER.setNetwork(network);
            CONTAINER.setNetworkAliases(List.of("kafka-container"));
            CONTAINER.withCreateContainerCmdModifier(
                    createContainerCmd -> createContainerCmd.withName("kafka-container-2"));
            CONTAINER.start();

            adminClient = AdminClient.create(
                    ImmutableMap.of(
                            AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, CONTAINER.getBootstrapServers()
                    )
            );
        }
        return CONTAINER;
    }

    @SneakyThrows
    public KafkaTestContainer registerTopics(List<String> topics) {
        log.info("[CONTAINER_WRAPPER] CREATING TOPICS: {}", topics);
        var newTopics = topics.stream()
                        .map(name -> new NewTopic(name, 3, (short) 1))
                        .toList();
        adminClient.createTopics(newTopics).all().get();
        waitUntilTopics(new HashSet<>(topics));
        topicsRegistered.addAll(topics);
        log.info("[CONTAINER_WRAPPER] TOPICS CREATED");
        return this;
    }


    @SneakyThrows
    public KafkaTestContainer deleteAllTopics() {
        var topics = adminClient.listTopics().names().get();
        var allTopics = adminClient.listTopics().names().get();
        log.info("[KAFKA] PRESENT TOPICS: {}", allTopics);
        adminClient.deleteTopics(topics).all().get();
        waitUntilTopics(empty);
        return this;
    }


    private void waitUntilTopics(Set<String> topicNames) {
        log.info("[KAFKA] Topics wanted :{}", topicNames);
        Awaitility.await().untilAsserted(() -> {
            var existingTopics = adminClient.listTopics().listings().get()
                    .stream()
                    .map(TopicListing::name)
                    .collect(Collectors.toSet());
            log.info("[KAFKA] Topics found :{}", existingTopics);

            assertTrue(existingTopics.containsAll(topicNames));

            log.info("[KAFKA] Waiting for topic lists to align");
            Thread.sleep(1000, 0);
        });
    }

    public void resetTopics() {
        deleteAllTopics();
        waitUntilTopics(empty);
        registerTopics(topicsRegistered);
    }
}
