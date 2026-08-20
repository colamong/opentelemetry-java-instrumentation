/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.javaagent.instrumentation.kafkaconnect.v2_6;

import static java.util.stream.Collectors.toCollection;

import io.opentelemetry.context.Context;
import io.opentelemetry.instrumentation.api.util.VirtualField;
import io.opentelemetry.instrumentation.kafkaclients.common.v0_11.internal.KafkaConsumerContext;
import io.opentelemetry.instrumentation.kafkaclients.common.v0_11.internal.KafkaConsumerContextUtil;
import io.opentelemetry.instrumentation.kafkaclients.common.v0_11.internal.KafkaInstrumenterFactory;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import javax.annotation.Nullable;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.connect.sink.SinkRecord;

public class KafkaConnectTask {

  // A JDK type is used as the field type because sink tasks run in a Kafka Connect plugin
  // classloader, which has its own copies of the instrumentation helper classes.
  private static final VirtualField<SinkRecord, Consumer<Boolean>> RECEIVE_DELIVERY_FIELD =
      VirtualField.find(SinkRecord.class, Consumer.class);

  private final Collection<SinkRecord> records;
  @Nullable private KafkaConnectBatchRecordAttributes batchRecordAttributes;
  private final Object taskIdentity;

  public KafkaConnectTask(Collection<SinkRecord> records, Object taskIdentity) {
    this.records = records;
    this.taskIdentity = taskIdentity;
  }

  public Collection<SinkRecord> getRecords() {
    return records;
  }

  // both the attributes extractor and the span links extractor need this, and they are always
  // called on the same thread while the span is being started
  KafkaConnectBatchRecordAttributes getBatchRecordAttributes() {
    if (batchRecordAttributes == null) {
      batchRecordAttributes = KafkaConnectBatchRecordAttributes.create(records);
    }
    return batchRecordAttributes;
  }

  Object getTaskIdentity() {
    return taskIdentity;
  }

  public static void copyReceiveOperation(ConsumerRecord<?, ?> source, SinkRecord target) {
    KafkaConsumerContext consumerContext = KafkaConsumerContextUtil.get(source);
    Context context = consumerContext.getContext();
    if (context != null && KafkaConsumerContextUtil.hasReceiveOperation(context)) {
      RECEIVE_DELIVERY_FIELD.set(
          target, KafkaInstrumenterFactory.createDeliveryTracker(consumerContext, source));
    }
  }

  boolean wasCountedByReceiveOperation() {
    if (records.isEmpty()) {
      return false;
    }
    for (SinkRecord record : records) {
      if (RECEIVE_DELIVERY_FIELD.get(record) == null) {
        return false;
      }
    }
    return true;
  }

  List<Consumer<Boolean>> getReceiveDeliveryTrackers() {
    List<Consumer<Boolean>> trackers = new ArrayList<>();
    for (SinkRecord record : records) {
      Consumer<Boolean> tracker = RECEIVE_DELIVERY_FIELD.get(record);
      if (tracker != null) {
        trackers.add(tracker);
      }
    }
    return trackers;
  }

  private Set<String> getTopics() {
    return records.stream().map(SinkRecord::topic).collect(toCollection(LinkedHashSet::new));
  }

  @Nullable
  public String getDestinationName() {
    Set<String> topics = getTopics();
    if (topics.isEmpty()) {
      return null;
    }
    // Return the topic name only if all records are from the same topic.
    // When records are from multiple topics, return null as there is no standard way
    // to represent multiple destination names in messaging.destination.name attribute.
    if (topics.size() == 1) {
      return topics.iterator().next();
    }
    return null;
  }
}
