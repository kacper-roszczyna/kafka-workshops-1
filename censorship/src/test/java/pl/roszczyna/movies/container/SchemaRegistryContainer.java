package pl.roszczyna.movies.container;

import lombok.NonNull;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;

public class SchemaRegistryContainer extends GenericContainer<SchemaRegistryContainer> {
    public static final String SCHEMA_REGISTRY_IMAGE =
            "confluentinc/cp-schema-registry:7.0.1";

    public static final int SCHEMA_REGISTRY_PORT = 8081;
    private static SchemaRegistryContainer CONTAINER;

    public static SchemaRegistryContainer get(Network network, String kafkaContainerNetworkAlias) {
        if (CONTAINER != null) {
            return CONTAINER;
        }
        CONTAINER = new SchemaRegistryContainer(SCHEMA_REGISTRY_IMAGE)
                .withNetwork(network)
                .withNetworkAliases("schema-registry")
                .withCreateContainerCmdModifier(
                        createContainerCmd -> createContainerCmd.withName("schema-registry-2"))
                .withEnv("SCHEMA_REGISTRY_HOST_NAME", "schema-registry")
                .withEnv("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:8081")
                .withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS",
                        "PLAINTEXT://" + kafkaContainerNetworkAlias + ":9092")
                .withExposedPorts(SCHEMA_REGISTRY_PORT)
                .waitingFor(Wait.forHttp("/subjects").forStatusCode(200));

        CONTAINER.start();
        return CONTAINER;
    }

    private SchemaRegistryContainer(@NonNull String dockerImageName) {
        super(dockerImageName);
    }

    public String getSchemaRegistryUrl() {
        return "http://" + getHost() + ":" + getFirstMappedPort();
    }
}
