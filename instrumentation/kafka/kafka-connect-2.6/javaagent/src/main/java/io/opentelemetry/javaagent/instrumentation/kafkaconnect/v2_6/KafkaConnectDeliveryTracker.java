/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.javaagent.instrumentation.kafkaconnect.v2_6;

import io.opentelemetry.instrumentation.api.internal.cache.Cache;
import java.util.ArrayList;
import java.util.List;
import org.apache.kafka.connect.sink.SinkRecord;

class KafkaConnectDeliveryTracker {

  private static final int MAX_PENDING_FAILED_DELIVERIES = 1024;

  private final Cache<Object, Cache<String, Boolean>> pendingFailedDeliveries = Cache.weak();

  KafkaConnectDeliveryTracker() {}

  DeliveryState start(KafkaConnectTask task) {
    List<String> deliveryKeys = deliveryKeys(task);
    Cache<String, Boolean> taskPendingFailedDeliveries =
        pendingFailedDeliveries.computeIfAbsent(
            task.getTaskIdentity(), unused -> Cache.bounded(MAX_PENDING_FAILED_DELIVERIES));
    return new DeliveryState(
        deliveryKeys,
        taskPendingFailedDeliveries,
        deliveryKeys.stream()
            .filter(deliveryKey -> taskPendingFailedDeliveries.get(deliveryKey) == null)
            .count());
  }

  void end(DeliveryState state, boolean successful) {
    for (String deliveryKey : state.deliveryKeys) {
      if (successful) {
        state.pendingFailedDeliveries.remove(deliveryKey);
      } else {
        state.pendingFailedDeliveries.put(deliveryKey, true);
      }
    }
  }

  private static List<String> deliveryKeys(KafkaConnectTask task) {
    List<String> deliveryKeys = new ArrayList<>();
    for (SinkRecord record : task.getRecords()) {
      String topic = record.topic();
      deliveryKeys.add(
          topic.length()
              + ":"
              + topic
              + ":"
              + record.kafkaPartition()
              + ":"
              + record.kafkaOffset());
    }
    return deliveryKeys;
  }

  static class DeliveryState {
    private final List<String> deliveryKeys;
    private final Cache<String, Boolean> pendingFailedDeliveries;
    private final long consumedMessagesCount;

    private DeliveryState(
        List<String> deliveryKeys,
        Cache<String, Boolean> pendingFailedDeliveries,
        long consumedMessagesCount) {
      this.deliveryKeys = deliveryKeys;
      this.pendingFailedDeliveries = pendingFailedDeliveries;
      this.consumedMessagesCount = consumedMessagesCount;
    }

    long getConsumedMessagesCount() {
      return consumedMessagesCount;
    }
  }
}
