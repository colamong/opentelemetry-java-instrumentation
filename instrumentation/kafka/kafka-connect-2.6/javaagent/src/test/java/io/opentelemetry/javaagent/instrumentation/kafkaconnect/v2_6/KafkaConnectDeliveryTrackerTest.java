/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.javaagent.instrumentation.kafkaconnect.v2_6;

import static io.opentelemetry.instrumentation.testing.junit.messaging.KafkaMessagingMetricsAssertions.assertProcessDurationMetrics;
import static io.opentelemetry.instrumentation.testing.junit.messaging.KafkaMessagingMetricsAssertions.assertReceiveMetrics;
import static io.opentelemetry.instrumentation.testing.junit.messaging.KafkaMessagingMetricsAssertions.assertTotalConsumedMessages;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.context.Context;
import io.opentelemetry.instrumentation.api.instrumenter.Instrumenter;
import io.opentelemetry.instrumentation.kafkaclients.common.v0_11.internal.KafkaConsumerContext;
import io.opentelemetry.instrumentation.kafkaclients.common.v0_11.internal.KafkaConsumerContextUtil;
import io.opentelemetry.instrumentation.kafkaclients.common.v0_11.internal.KafkaInstrumenterFactory;
import io.opentelemetry.instrumentation.kafkaclients.common.v0_11.internal.KafkaReceiveRequest;
import io.opentelemetry.instrumentation.testing.junit.AgentInstrumentationExtension;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class KafkaConnectDeliveryTrackerTest {

  @RegisterExtension
  static final AgentInstrumentationExtension testing = AgentInstrumentationExtension.create();

  @Test
  void countsRetriedBatchOnce() {
    List<SinkRecord> records = asList(sinkRecord("topic", 0, 10), sinkRecord("topic", 0, 11));
    RetryingSinkTask task = new RetryingSinkTask(2);

    assertThatThrownBy(() -> task.put(new ArrayList<>(records)))
        .isInstanceOf(RetriableException.class);
    assertThatThrownBy(() -> task.put(new ArrayList<>(records)))
        .isInstanceOf(RetriableException.class);
    task.put(new ArrayList<>(records));

    assertProcessDurationMetrics(
        testing,
        "io.opentelemetry.kafka-connect-2.6",
        "topic",
        null,
        null,
        2,
        RetriableException.class.getName());
    assertProcessDurationMetrics(
        testing, "io.opentelemetry.kafka-connect-2.6", "topic", null, null, 1, null);
    assertTotalConsumedMessages(testing, "io.opentelemetry.kafka-connect-2.6", 2);
  }

  @Test
  void countsSubsetRetryOnce() {
    SinkRecord first = sinkRecord("subsetTopic", 0, 10);
    SinkRecord second = sinkRecord("subsetTopic", 0, 11);
    RetryingSinkTask task = new RetryingSinkTask(1);

    assertThatThrownBy(() -> task.put(asList(first, second)))
        .isInstanceOf(RetriableException.class);
    task.put(singletonList(second));

    assertTotalConsumedMessages(testing, "io.opentelemetry.kafka-connect-2.6", 2);
  }

  @Test
  void countsRetryCombinedWithNewRecordOnce() {
    SinkRecord retried = sinkRecord("combinedTopic", 0, 10);
    SinkRecord newRecord = sinkRecord("combinedTopic", 0, 11);
    RetryingSinkTask task = new RetryingSinkTask(1);

    assertThatThrownBy(() -> task.put(singletonList(retried)))
        .isInstanceOf(RetriableException.class);
    task.put(asList(retried, newRecord));

    assertTotalConsumedMessages(testing, "io.opentelemetry.kafka-connect-2.6", 2);
  }

  @Test
  @SuppressWarnings("unchecked")
  void receiveOperationCountsFailedSinkDeliveryOnce() {
    String instrumentationName = "test-kafka-connect-receive";
    Instrumenter<KafkaReceiveRequest, Void> receiveInstrumenter =
        new KafkaInstrumenterFactory(GlobalOpenTelemetry.get(), instrumentationName)
            .setMessagingReceiveTelemetryEnabled(true)
            .createConsumerReceiveInstrumenter();
    Consumer<String, String> consumer = mock(Consumer.class);
    ConsumerRecord<String, String> source = receive(receiveInstrumenter, consumer, 10);
    SinkRecord sinkRecord = sinkRecord(source.topic(), source.partition(), source.offset());
    RetryingSinkTask task = new RetryingSinkTask(1);
    KafkaConsumerContext consumerContext = KafkaConsumerContextUtil.get(source);
    setReceiveDeliveryTracker(
        sinkRecord, KafkaInstrumenterFactory.createDeliveryTracker(consumerContext, source));

    assertThatThrownBy(() -> task.put(singletonList(sinkRecord)))
        .isInstanceOf(RetriableException.class);
    receive(receiveInstrumenter, consumer, 10);

    assertReceiveMetrics(testing, instrumentationName, "receiveOwnedTopic", null, null, 2, 1, null);
  }

  @Test
  void scopesFailedDeliveriesToTask() {
    List<SinkRecord> records =
        asList(sinkRecord("otherTopic", 0, 10), sinkRecord("otherTopic", 0, 11));
    RetryingSinkTask failingTask = new RetryingSinkTask(1);
    RetryingSinkTask otherTask = new RetryingSinkTask(0);

    assertThatThrownBy(() -> failingTask.put(new ArrayList<>(records)))
        .isInstanceOf(RetriableException.class);
    otherTask.put(new ArrayList<>(records));

    // the failed delivery is remembered per sink task, so the other task's delivery of the same
    // record positions is counted instead of being mistaken for a retry
    assertTotalConsumedMessages(testing, "io.opentelemetry.kafka-connect-2.6", 4);
  }

  @Test
  void countsLargeFailedBatchRetryOnce() {
    List<SinkRecord> records = new ArrayList<>();
    for (int i = 0; i < 1100; i++) {
      records.add(sinkRecord("overflowTopic", 0, i));
    }
    RetryingSinkTask task = new RetryingSinkTask(1);

    assertThatThrownBy(() -> task.put(new ArrayList<>(records)))
        .isInstanceOf(RetriableException.class);
    task.put(new ArrayList<>(records));

    // all 1100 records should be counted exactly once: the retry does not add to the count
    assertTotalConsumedMessages(testing, "io.opentelemetry.kafka-connect-2.6", 1100);
  }

  @Test
  @SuppressWarnings("unchecked")
  void receiveOperationCountsLargeFailedBatchRetryOnce() {
    String instrumentationName = "test-kafka-connect-receive-large";
    Instrumenter<KafkaReceiveRequest, Void> receiveInstrumenter =
        new KafkaInstrumenterFactory(GlobalOpenTelemetry.get(), instrumentationName)
            .setMessagingReceiveTelemetryEnabled(true)
            .createConsumerReceiveInstrumenter();
    Consumer<String, String> consumer = mock(Consumer.class);

    // First receive: 1100 records, none pending-failed → 1100 counted
    List<ConsumerRecord<String, String>> sources =
        receiveMultiple(receiveInstrumenter, consumer, 1100);
    List<SinkRecord> sinkRecords = new ArrayList<>();
    for (ConsumerRecord<String, String> source : sources) {
      SinkRecord sink = sinkRecord(source.topic(), source.partition(), source.offset());
      KafkaConnectTask.copyReceiveOperation(source, sink);
      sinkRecords.add(sink);
    }

    RetryingSinkTask task = new RetryingSinkTask(1);
    assertThatThrownBy(() -> task.put(new ArrayList<>(sinkRecords)))
        .isInstanceOf(RetriableException.class);

    // Re-receive same records: all pending-failed → 0 counted
    receiveMultiple(receiveInstrumenter, consumer, 1100);

    assertReceiveMetrics(
        testing, instrumentationName, "receiveOwnedLargeTopic", null, null, 2, 1100, null);
  }

  private static List<ConsumerRecord<String, String>> receiveMultiple(
      Instrumenter<KafkaReceiveRequest, Void> receiveInstrumenter,
      Consumer<String, String> consumer,
      int count) {
    String topic = "receiveOwnedLargeTopic";
    int partition = 0;
    List<ConsumerRecord<String, String>> sourceList = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      sourceList.add(new ConsumerRecord<>(topic, partition, i, "key", "value"));
    }
    ConsumerRecords<String, String> records =
        new ConsumerRecords<>(singletonMap(new TopicPartition(topic, partition), sourceList));
    KafkaReceiveRequest request = KafkaReceiveRequest.create(records, consumer);
    Context parentContext = Context.root();
    Context receiveContext = receiveInstrumenter.start(parentContext, request);
    receiveInstrumenter.end(receiveContext, request, null, null);

    KafkaConsumerContext consumerContext =
        KafkaConsumerContextUtil.create(
            KafkaConsumerContextUtil.withReceiveOperation(parentContext, true), consumer);
    for (ConsumerRecord<String, String> source : sourceList) {
      KafkaConsumerContextUtil.set(source, consumerContext);
    }
    return sourceList;
  }

  private static SinkRecord sinkRecord(String topic, int partition, long offset) {
    return new SinkRecord(topic, partition, null, null, null, null, offset);
  }

  private static ConsumerRecord<String, String> receive(
      Instrumenter<KafkaReceiveRequest, Void> receiveInstrumenter,
      Consumer<String, String> consumer,
      long offset) {
    String topic = "receiveOwnedTopic";
    int partition = 0;
    ConsumerRecord<String, String> source =
        new ConsumerRecord<>(topic, partition, offset, "key", "value");
    ConsumerRecords<String, String> records =
        new ConsumerRecords<>(
            singletonMap(new TopicPartition(topic, partition), singletonList(source)));
    KafkaReceiveRequest request = KafkaReceiveRequest.create(records, consumer);
    Context parentContext = Context.root();
    Context receiveContext = receiveInstrumenter.start(parentContext, request);
    receiveInstrumenter.end(receiveContext, request, null, null);

    KafkaConsumerContext consumerContext =
        KafkaConsumerContextUtil.create(
            KafkaConsumerContextUtil.withReceiveOperation(parentContext, true), consumer);
    KafkaConsumerContextUtil.set(source, consumerContext);
    return source;
  }

  private static void setReceiveDeliveryTracker(
      SinkRecord sinkRecord, java.util.function.Consumer<Boolean> deliveryTracker) {
    String fieldSuffix =
        SinkRecord.class.getName().replace('.', '$')
            + "$"
            + java.util.function.Consumer.class.getName().replace('.', '$');
    try {
      // Test classes are not rewritten to use the javaagent's field-backed VirtualField.
      Class<?> accessor =
          Class.forName(
              "io.opentelemetry.javaagent.bootstrap.field.VirtualFieldAccessor$" + fieldSuffix,
              false,
              SinkRecord.class.getClassLoader());
      accessor
          .getMethod("__set__opentelemetryVirtualField$" + fieldSuffix, Object.class)
          .invoke(sinkRecord, deliveryTracker);
    } catch (ReflectiveOperationException e) {
      throw new LinkageError("Could not set the receive delivery tracker", e);
    }
  }

  private static class RetryingSinkTask extends SinkTask {
    private final int failures;
    private int attempts;

    RetryingSinkTask(int failures) {
      this.failures = failures;
    }

    @Override
    public String version() {
      return "test";
    }

    @Override
    public void start(Map<String, String> properties) {}

    @Override
    public void put(Collection<SinkRecord> records) {
      if (attempts++ < failures) {
        throw new RetriableException("retry");
      }
    }

    @Override
    public void stop() {}
  }
}
