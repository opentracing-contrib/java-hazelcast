/*
 * Copyright 2018-2019 The OpenTracing Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package io.opentracing.contrib.hazelcast;

import static io.opentracing.contrib.hazelcast.TracingHelper.decorateAction;
import static io.opentracing.contrib.hazelcast.TracingHelper.extract;
import static io.opentracing.contrib.hazelcast.TracingHelper.nullable;
import static io.opentracing.contrib.hazelcast.TracingHelper.nullableClass;

import com.hazelcast.core.Message;
import com.hazelcast.core.MessageListener;
import io.opentracing.Span;
import io.opentracing.SpanContext;

public class TracingMessageListener<E> implements MessageListener<TracingMessage<E>> {

  private final MessageListener<E> listener;
  private final String topic;
  private final boolean traceWithActiveSpanOnly;

  public TracingMessageListener(MessageListener<E> listener, String topic,
      boolean traceWithActiveSpanOnly) {
    this.listener = listener;
    this.topic = topic;
    this.traceWithActiveSpanOnly = traceWithActiveSpanOnly;
  }

  @Override
  public void onMessage(Message<TracingMessage<E>> message) {
    SpanContext parent = extract(message.getMessageObject().getSpanContextMap());
    Span span = TracingHelper.buildSpan("onMessage", parent, traceWithActiveSpanOnly);
    span.setTag("message", nullable(message.getMessageObject().getMessage()));
    span.setTag("publishTime", message.getPublishTime());
    span.setTag("topic", topic);
    span.setTag("listener", nullableClass(listener));
    span.setTag("publishingMember", nullable(message.getPublishingMember().getAddress()));

    decorateAction(
        () -> listener.onMessage(new Message<>(topic, message.getMessageObject().getMessage(),
            message.getPublishTime(), message.getPublishingMember())), span);
  }
}
