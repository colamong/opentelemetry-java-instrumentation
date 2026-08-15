/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.javaagent.instrumentation.kafkaconnect.v2_6;

import static io.opentelemetry.instrumentation.api.incubator.semconv.messaging.internal.MessagingExceptionEventExtractors.setMessagingProcessExceptionEventExtractor;
import static io.opentelemetry.instrumentation.api.internal.SemconvStability.emitStableMessagingSemconv;
import static io.opentelemetry.semconv.ErrorAttributes.ERROR_TYPE;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.ContextKey;
import io.opentelemetry.context.propagation.TextMapPropagator;
import io.opentelemetry.instrumentation.api.incubator.semconv.messaging.MessagingAttributesExtractor;
import io.opentelemetry.instrumentation.api.incubator.semconv.messaging.MessagingConsumerMetrics;
import io.opentelemetry.instrumentation.api.incubator.semconv.messaging.MessagingOperationType;
import io.opentelemetry.instrumentation.api.incubator.semconv.messaging.MessagingProcessMetrics;
import io.opentelemetry.instrumentation.api.incubator.semconv.messaging.MessagingSpanKindExtractor;
import io.opentelemetry.instrumentation.api.incubator.semconv.messaging.MessagingSpanNameExtractor;
import io.opentelemetry.instrumentation.api.instrumenter.Instrumenter;
import io.opentelemetry.instrumentation.api.instrumenter.InstrumenterBuilder;
import io.opentelemetry.instrumentation.api.instrumenter.OperationListener;
import io.opentelemetry.instrumentation.api.instrumenter.OperationMetrics;
import java.util.List;
import java.util.function.Consumer;

public class KafkaConnectSingletons {

  private static final String INSTRUMENTATION_NAME = "io.opentelemetry.kafka-connect-2.6";
  private static final String PROCESS_OPERATION_NAME = "process";
  private static final ContextKey<KafkaConnectDeliveryTracker.DeliveryState>
      CONSUMED_MESSAGES_DELIVERY_STATE =
          ContextKey.named("opentelemetry-kafka-connect-consumed-messages-delivery");
  private static final ContextKey<List<Consumer<Boolean>>> RECEIVE_OWNED_DELIVERY_TRACKERS =
      ContextKey.named("opentelemetry-kafka-connect-receive-owned-delivery");
  private static final TextMapPropagator propagator =
      GlobalOpenTelemetry.get().getPropagators().getTextMapPropagator();
  private static final KafkaConnectDeliveryTracker deliveryTracker =
      new KafkaConnectDeliveryTracker();
  private static final OperationMetrics consumedMessagesMetrics =
      meter -> {
        OperationListener delegate = MessagingConsumerMetrics.getConsumedMessages().create(meter);
        return new OperationListener() {
          @Override
          public Context onStart(Context context, Attributes startAttributes, long startNanos) {
            KafkaConnectDeliveryTracker.DeliveryState state =
                context.get(CONSUMED_MESSAGES_DELIVERY_STATE);
            return state != null && state.shouldCountConsumedMessages()
                ? delegate.onStart(context, startAttributes, startNanos)
                : context;
          }

          @Override
          public void onEnd(Context context, Attributes endAttributes, long endNanos) {
            boolean successful = endAttributes.get(ERROR_TYPE) == null;
            List<Consumer<Boolean>> receiveOwnedTrackers =
                context.get(RECEIVE_OWNED_DELIVERY_TRACKERS);
            if (receiveOwnedTrackers != null) {
              for (Consumer<Boolean> receiveOwnedTracker : receiveOwnedTrackers) {
                receiveOwnedTracker.accept(successful);
              }
            }

            KafkaConnectDeliveryTracker.DeliveryState state =
                context.get(CONSUMED_MESSAGES_DELIVERY_STATE);
            if (state == null) {
              return;
            }
            if (state.shouldCountConsumedMessages()) {
              delegate.onEnd(context, endAttributes, endNanos);
            }
            deliveryTracker.end(state, successful);
          }
        };
      };

  private static final Instrumenter<KafkaConnectTask, Void> instrumenter;

  static {
    KafkaConnectBatchProcessSpanLinksExtractor spanLinksExtractor =
        new KafkaConnectBatchProcessSpanLinksExtractor(propagator);

    InstrumenterBuilder<KafkaConnectTask, Void> builder =
        Instrumenter.<KafkaConnectTask, Void>builder(
                GlobalOpenTelemetry.get(),
                INSTRUMENTATION_NAME,
                MessagingSpanNameExtractor.create(
                    new KafkaConnectAttributesGetter(),
                    MessagingOperationType.PROCESS,
                    PROCESS_OPERATION_NAME))
            .addAttributesExtractor(
                MessagingAttributesExtractor.create(
                    new KafkaConnectAttributesGetter(),
                    MessagingOperationType.PROCESS,
                    PROCESS_OPERATION_NAME))
            .addAttributesExtractor(new KafkaConnectBatchAttributesExtractor())
            .addSpanLinksExtractor(spanLinksExtractor)
            .addOperationMetrics(MessagingProcessMetrics.get());
    // The worker task usually polls an instrumented KafkaConsumer, whose receive operation owns the
    // consumed message count when it runs. This operation owns deliveries that were not counted
    // there, including when kafka-clients instrumentation is disabled independently.
    if (emitStableMessagingSemconv()) {
      builder
          .addContextCustomizer(
              (context, request, startAttributes) ->
                  request.wasCountedByReceiveOperation()
                      ? context.with(
                          RECEIVE_OWNED_DELIVERY_TRACKERS, request.getReceiveDeliveryTrackers())
                      : context.with(
                          CONSUMED_MESSAGES_DELIVERY_STATE, deliveryTracker.start(request)))
          .addOperationMetrics(consumedMessagesMetrics);
    }
    setMessagingProcessExceptionEventExtractor(builder);

    instrumenter =
        builder.buildInstrumenter(
            MessagingSpanKindExtractor.create(MessagingOperationType.PROCESS));
  }

  public static Instrumenter<KafkaConnectTask, Void> instrumenter() {
    return instrumenter;
  }

  private KafkaConnectSingletons() {}
}
