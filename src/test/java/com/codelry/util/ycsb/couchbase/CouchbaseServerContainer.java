package com.codelry.util.ycsb.couchbase;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * Couchbase Server Testcontainers helper, adapted from couchbase-connect-sdk3 Server tests.
 */
final class CouchbaseServerContainer {
  static final String IMAGE = "couchbase/server:enterprise-8.0.1";
  private static final Logger LOGGER = LoggerFactory.getLogger(CouchbaseServerContainer.class);

  private CouchbaseServerContainer() {}

  static GenericContainer<?> startDedicatedContainer() {
    releaseFixedPorts();
    GenericContainer<?> container = createDedicated();
    container.start();
    return container;
  }

  static void stopContainer(GenericContainer<?> container) {
    if (container == null) {
      return;
    }
    try {
      if (container.isRunning()) {
        container.stop();
      }
    } finally {
      container.close();
    }
  }

  static void releaseFixedPorts() {
    stopDockerContainersOnPort(8091);
  }

  private static void stopDockerContainersOnPort(int port) {
    try {
      Process process = new ProcessBuilder("docker", "ps", "-q", "--filter", "publish=" + port)
          .redirectErrorStream(true)
          .start();
      String output = new String(process.getInputStream().readAllBytes()).trim();
      process.waitFor();
      if (output.isEmpty()) {
        return;
      }
      for (String containerId : output.split("\\R")) {
        if (containerId.isBlank()) {
          continue;
        }
        LOGGER.debug("Stopping container {} occupying port {}", containerId, port);
        new ProcessBuilder("docker", "rm", "-f", containerId).inheritIO().start().waitFor();
      }
    } catch (Exception e) {
      LOGGER.debug("Unable to stop containers on port {}: {}", port, e.getMessage());
    }
  }

  static GenericContainer<?> createDedicated() {
    return configureContainer(new GenericContainer<>(DockerImageName.parse(IMAGE)));
  }

  private static GenericContainer<?> configureContainer(GenericContainer<?> container) {
    container.withCreateContainerCmdModifier(cmd -> cmd.getHostConfig()
            .withMemory(4L * 1024 * 1024 * 1024))
        .waitingFor(Wait.forHttp("/ui/index.html")
            .forPort(8091)
            .withStartupTimeout(Duration.ofMinutes(10)));
    exposeFixedPorts(container);
    return container;
  }

  // Matches docker-compose port mappings:
  // 8091-8096, 9123, 11207, 11210, 11280, 18091-18097
  private static void exposeFixedPorts(GenericContainer<?> container) {
    int[] ports = {
        8091, 8092, 8093, 8094, 8095, 8096,
        9123,
        11207, 11210, 11280,
        18091, 18092, 18093, 18094, 18095, 18096, 18097
    };
    List<String> bindings = new ArrayList<>();
    for (int port : ports) {
      bindings.add(port + ":" + port);
    }
    container.setPortBindings(bindings);
    container.withExposedPorts(toPortArray(ports));
  }

  private static Integer[] toPortArray(int[] ports) {
    Integer[] boxed = new Integer[ports.length];
    for (int i = 0; i < ports.length; i++) {
      boxed[i] = ports[i];
    }
    return boxed;
  }
}
