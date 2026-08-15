/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.javaagent.instrumentation.kafkaconnect.v2_6;

import io.opentelemetry.instrumentation.api.internal.cache.Cache;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.kafka.connect.sink.SinkRecord;

class KafkaConnectDeliveryTracker {

  private static final int MAX_PENDING_FAILED_DELIVERIES = 1024;

  private final Cache<Object, Cache<String, Boolean>> pendingFailedDeliveries = Cache.weak();

  KafkaConnectDeliveryTracker() {}

  DeliveryState start(KafkaConnectTask task) {
    String deliveryKey = deliveryKey(task);
    Cache<String, Boolean> taskPendingFailedDeliveries =
        pendingFailedDeliveries.computeIfAbsent(
            task.getTaskIdentity(), unused -> Cache.bounded(MAX_PENDING_FAILED_DELIVERIES));
    return new DeliveryState(
        deliveryKey,
        taskPendingFailedDeliveries,
        taskPendingFailedDeliveries.get(deliveryKey) == null);
  }

  void end(DeliveryState state, boolean successful) {
    if (successful) {
      state.pendingFailedDeliveries.remove(state.deliveryKey);
    } else {
      state.pendingFailedDeliveries.put(state.deliveryKey, true);
    }
  }

  private static String deliveryKey(KafkaConnectTask task) {
    List<String> positions = new ArrayList<>();
    for (SinkRecord record : task.getRecords()) {
      String topic = record.topic();
      positions.add(
          topic.length()
              + ":"
              + topic
              + ":"
              + record.kafkaPartition()
              + ":"
              + record.kafkaOffset());
    }
    Collections.sort(positions);

    StringBuilder key = new StringBuilder();
    for (String position : positions) {
      key.append(position).append('|');
    }
    return key.toString();
  }

  static class DeliveryState {
    private final String deliveryKey;
    private final Cache<String, Boolean> pendingFailedDeliveries;
    private final boolean countConsumedMessages;

    private DeliveryState(
        String deliveryKey,
        Cache<String, Boolean> pendingFailedDeliveries,
        boolean countConsumedMessages) {
      this.deliveryKey = deliveryKey;
      this.pendingFailedDeliveries = pendingFailedDeliveries;
      this.countConsumedMessages = countConsumedMessages;
    }

    boolean shouldCountConsumedMessages() {
      return countConsumedMessages;
    }
  }
}
