/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.instrumentation.kafkaconnect.v2_6;

import static io.opentelemetry.instrumentation.testing.junit.messaging.KafkaMessagingMetricsAssertions.assertConsumedMessagesMetrics;
import static io.opentelemetry.instrumentation.testing.junit.messaging.KafkaMessagingMetricsAssertions.assertProcessDurationMetrics;
import static io.restassured.RestAssured.given;
import static org.awaitility.Awaitility.await;

import io.restassured.http.ContentType;
import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.errors.RetriableException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

@EnabledIfSystemProperty(
    named = "otel.instrumentation.messaging.experimental.receive-telemetry.enabled",
    matches = "true")
class RetryKafkaConnectSinkTaskTest extends KafkaConnectSinkTaskBaseTest {

  private static final String CONNECTOR_NAME = "retry-sink-connector";
  private static final String TOPIC_NAME = "retry-sink-topic";
  private static final String ATTEMPTED_PATH = "/tmp/retry-sink-attempted";
  private static final String COMPLETED_PATH = "/tmp/retry-sink-completed";

  @Override
  protected void setupDatabaseContainer() {}

  @Override
  protected void startDatabaseContainer() {}

  @Override
  protected void stopDatabaseContainer() {}

  @Override
  protected void clearDatabaseData() {}

  @Override
  protected String getConnectorInstallCommand() {
    return "true";
  }

  @Override
  protected String getConnectorName() {
    return CONNECTOR_NAME;
  }

  @Test
  void testRetriedMessageIsConsumedOnce() throws IOException {
    setupRetrySinkConnector();
    awaitForTopicCreation(TOPIC_NAME);
    testing.clearData();

    Properties properties = new Properties();
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, getKafkaBootstrapServers());
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

    try (Producer<String, String> producer = instrument(new KafkaProducer<>(properties))) {
      producer.send(new ProducerRecord<>(TOPIC_NAME, "key", "value"));
      producer.flush();
    }

    await()
        .atMost(Duration.ofSeconds(60))
        .until(() -> kafkaConnect.execInContainer("test", "-f", COMPLETED_PATH).getExitCode() == 0);

    assertProcessDurationMetrics(
        testing,
        "io.opentelemetry.kafka-connect-2.6",
        TOPIC_NAME,
        null,
        null,
        1,
        RetriableException.class.getName());
    assertProcessDurationMetrics(
        testing, "io.opentelemetry.kafka-connect-2.6", TOPIC_NAME, null, null, 1, null);
    assertConsumedMessagesMetrics(
        testing,
        "io.opentelemetry.kafka-clients-0.11",
        TOPIC_NAME,
        "connect-" + CONNECTOR_NAME,
        null,
        1,
        null);
  }

  private void setupRetrySinkConnector() throws IOException {
    Map<String, Object> config = new HashMap<>();
    config.put("connector.class", RetrySinkConnector.class.getName());
    config.put("tasks.max", "1");
    config.put("topics", TOPIC_NAME);
    config.put("key.converter", "org.apache.kafka.connect.storage.StringConverter");
    config.put("value.converter", "org.apache.kafka.connect.storage.StringConverter");
    config.put(RetrySinkConnector.ATTEMPTED_PATH_CONFIG, ATTEMPTED_PATH);
    config.put(RetrySinkConnector.COMPLETED_PATH_CONFIG, COMPLETED_PATH);

    Map<String, Object> payload = new HashMap<>();
    payload.put("name", CONNECTOR_NAME);
    payload.put("config", config);

    given()
        .contentType(ContentType.JSON)
        .body(mapper.writeValueAsString(payload))
        .when()
        .post(getKafkaConnectUrl() + "/connectors")
        .then()
        .statusCode(201);
  }
}
