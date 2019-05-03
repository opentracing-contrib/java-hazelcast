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
import static io.opentracing.contrib.hazelcast.TracingHelper.nullableClass;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.instance.Node;
import com.hazelcast.spi.NodeAware;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.spi.serialization.SerializationServiceAware;
import io.opentracing.Span;
import io.opentracing.SpanContext;
import java.io.Serializable;
import java.util.Map;

public class TracingRunnable implements Runnable, Serializable,
    HazelcastInstanceAware, NodeAware, SerializationServiceAware {

  private final Runnable runnable;
  private final boolean traceWithActiveSpanOnly;
  private final Map<String, String> spanContextMap;


  public TracingRunnable(Runnable runnable, boolean traceWithActiveSpanOnly,
      Map<String, String> spanContextMap) {
    this.runnable = runnable;
    this.traceWithActiveSpanOnly = traceWithActiveSpanOnly;
    this.spanContextMap = spanContextMap;
  }

  @Override
  public void run() {
    SpanContext parent = extract(spanContextMap);
    Span span = TracingHelper.buildSpan("process", parent, traceWithActiveSpanOnly);
    span.setTag("runnable", nullableClass(runnable));
    decorateAction(runnable::run, span);
  }

  @Override
  public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
    if (runnable instanceof HazelcastInstanceAware) {
      ((HazelcastInstanceAware) runnable).setHazelcastInstance(hazelcastInstance);
    }
  }

  @Override
  public void setNode(Node node) {
    if (runnable instanceof NodeAware) {
      ((NodeAware) runnable).setNode(node);
    }
  }

  @Override
  public void setSerializationService(
      SerializationService serializationService) {
    if (runnable instanceof SerializationServiceAware) {
      ((SerializationServiceAware) runnable).setSerializationService(serializationService);
    }
  }
}
