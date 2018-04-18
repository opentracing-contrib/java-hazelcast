/*
 * Copyright 2018 The OpenTracing Authors
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

import static io.opentracing.contrib.hazelcast.TracingHelper.decorate;
import static io.opentracing.contrib.hazelcast.TracingHelper.decorateAction;
import static io.opentracing.contrib.hazelcast.TracingHelper.inject;
import static io.opentracing.contrib.hazelcast.TracingHelper.nullable;
import static io.opentracing.contrib.hazelcast.TracingHelper.nullableClass;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ITopic;
import com.hazelcast.core.MessageListener;
import com.hazelcast.monitor.LocalTopicStats;
import io.opentracing.Span;
import java.util.Map;

public class TracingTopic<E> implements ITopic<E> {

  private final ITopic<TracingMessage<E>> topic;
  private final TracingHelper helper;
  private final boolean traceWithActiveSpanOnly;

  public TracingTopic(String topic, boolean reliable, HazelcastInstance instance,
      boolean traceWithActiveSpanOnly) {
    if (reliable) {
      this.topic = instance.getReliableTopic(topic);
    } else {
      this.topic = instance.getTopic(topic);
    }

    this.traceWithActiveSpanOnly = traceWithActiveSpanOnly;
    helper = new TracingHelper(traceWithActiveSpanOnly);
  }

  @Override
  public String getName() {
    return topic.getName();
  }

  @Override
  public void publish(E message) {
    Span span = helper.buildSpan("publish", topic);
    span.setTag("message", nullable(message));
    Map<String, String> spanContextMap = inject(span);
    decorateAction(() -> topic.publish(new TracingMessage<>(message, spanContextMap)), span);
  }

  @Override
  public String addMessageListener(MessageListener<E> listener) {
    Span span = helper.buildSpan("addMessageListener", topic);
    span.setTag("listener", nullableClass(listener));
    return decorate(() -> topic.addMessageListener(
        new TracingMessageListener<>(listener, topic.getName(), traceWithActiveSpanOnly)), span);
  }

  @Override
  public boolean removeMessageListener(String registrationId) {
    Span span = helper.buildSpan("removeMessageListener", topic);
    span.setTag("registrationId", nullable(registrationId));
    return decorate(() -> topic.removeMessageListener(registrationId), span);
  }

  @Override
  public LocalTopicStats getLocalTopicStats() {
    Span span = helper.buildSpan("getLocalTopicStats", topic);
    return decorate(topic::getLocalTopicStats, span);
  }

  @Override
  public String getPartitionKey() {
    return topic.getPartitionKey();
  }

  @Override
  public String getServiceName() {
    return topic.getServiceName();
  }

  @Override
  public void destroy() {
    Span span = helper.buildSpan("destroy", topic);
    decorateAction(topic::destroy, span);
  }


}
