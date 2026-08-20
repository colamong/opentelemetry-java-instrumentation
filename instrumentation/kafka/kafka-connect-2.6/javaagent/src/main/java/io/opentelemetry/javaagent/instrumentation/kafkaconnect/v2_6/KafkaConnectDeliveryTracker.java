/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.javaagent.instrumentation.kafkaconnect.v2_6;

import io.opentelemetry.instrumentation.api.internal.cache.Cache;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import org.apache.kafka.connect.sink.SinkRecord;

class KafkaConnectDeliveryTracker {

  // bounded so that a task that never recovers cannot grow this without limit
  private static final int MAX_PENDING_FAILED_OPERATIONS = 1024;

  private final Cache<Object, Deque<Set<String>>> pendingFailedDeliveries = Cache.weak();

  DeliveryState start(KafkaConnectTask task) {
    List<String> deliveryKeys = deliveryKeys(task);
    Deque<Set<String>> operations =
        pendingFailedDeliveries.computeIfAbsent(
            task.getTaskIdentity(), unused -> new ArrayDeque<>());
    long consumedMessagesCount;
    synchronized (operations) {
      consumedMessagesCount =
          deliveryKeys.stream().filter(key -> !isPendingFailed(operations, key)).count();
    }
    return new DeliveryState(deliveryKeys, operations, consumedMessagesCount);
  }

  void end(DeliveryState state, boolean successful) {
    synchronized (state.operations) {
      removeFromOperations(state.operations, state.deliveryKeys);
      if (!successful && !state.deliveryKeys.isEmpty()) {
        state.operations.addFirst(new HashSet<>(state.deliveryKeys));
        if (state.operations.size() > MAX_PENDING_FAILED_OPERATIONS) {
          state.operations.removeLast();
        }
      }
    }
  }

  private static boolean isPendingFailed(Deque<Set<String>> operations, String deliveryKey) {
    for (Set<String> operation : operations) {
      if (operation.contains(deliveryKey)) {
        return true;
      }
    }
    return false;
  }

  private static void removeFromOperations(
      Deque<Set<String>> operations, List<String> deliveryKeys) {
    Iterator<Set<String>> iterator = operations.iterator();
    while (iterator.hasNext()) {
      Set<String> operation = iterator.next();
      for (String key : deliveryKeys) {
        operation.remove(key);
      }
      if (operation.isEmpty()) {
        iterator.remove();
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
    private final Deque<Set<String>> operations;
    private final long consumedMessagesCount;

    private DeliveryState(
        List<String> deliveryKeys, Deque<Set<String>> operations, long consumedMessagesCount) {
      this.deliveryKeys = deliveryKeys;
      this.operations = operations;
      this.consumedMessagesCount = consumedMessagesCount;
    }

    long getConsumedMessagesCount() {
      return consumedMessagesCount;
    }
  }
}
