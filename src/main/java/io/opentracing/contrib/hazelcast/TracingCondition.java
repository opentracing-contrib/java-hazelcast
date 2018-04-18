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

import static io.opentracing.contrib.hazelcast.TracingHelper.decorateAction;
import static io.opentracing.contrib.hazelcast.TracingHelper.decorateActionExceptionally;
import static io.opentracing.contrib.hazelcast.TracingHelper.decorateExceptionally;
import static io.opentracing.contrib.hazelcast.TracingHelper.extract;
import static io.opentracing.contrib.hazelcast.TracingHelper.nullable;

import com.hazelcast.core.ICondition;
import io.opentracing.Span;
import io.opentracing.SpanContext;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class TracingCondition implements ICondition {

  private final ICondition condition;
  private final boolean traceWithActiveSpanOnly;
  private final Map<String, String> spanContextMap;
  private final String name;


  public TracingCondition(ICondition condition, String name, boolean traceWithActiveSpanOnly,
      Map<String, String> spanContextMap) {
    this.condition = condition;
    this.name = name;
    this.traceWithActiveSpanOnly = traceWithActiveSpanOnly;
    this.spanContextMap = spanContextMap;
  }

  @Override
  public void await() throws InterruptedException {
    SpanContext parent = extract(spanContextMap);
    Span span = TracingHelper.buildSpan("await", parent, traceWithActiveSpanOnly);
    span.setTag("condition", name);
    decorateActionExceptionally(condition::await, span);
  }

  @Override
  public void awaitUninterruptibly() {
    SpanContext parent = extract(spanContextMap);
    Span span = TracingHelper.buildSpan("awaitUninterruptibly", parent, traceWithActiveSpanOnly);
    span.setTag("condition", name);
    decorateAction(condition::awaitUninterruptibly, span);
  }

  @Override
  public long awaitNanos(long nanosTimeout) throws InterruptedException {
    SpanContext parent = extract(spanContextMap);
    Span span = TracingHelper.buildSpan("awaitNanos", parent, traceWithActiveSpanOnly);
    span.setTag("condition", name);
    span.setTag("nanosTimeout", nanosTimeout);
    return decorateExceptionally(() -> condition.awaitNanos(nanosTimeout), span);
  }

  @Override
  public boolean await(long time, TimeUnit unit) throws InterruptedException {
    SpanContext parent = extract(spanContextMap);
    Span span = TracingHelper.buildSpan("await", parent, traceWithActiveSpanOnly);
    span.setTag("condition", name);
    span.setTag("time", time);
    span.setTag("unit", nullable(unit));
    return decorateExceptionally(() -> condition.await(time, unit), span);
  }

  @Override
  public boolean awaitUntil(Date deadline) throws InterruptedException {
    SpanContext parent = extract(spanContextMap);
    Span span = TracingHelper.buildSpan("awaitUntil", parent, traceWithActiveSpanOnly);
    span.setTag("condition", name);
    span.setTag("deadline", nullable(deadline));
    return decorateExceptionally(() -> condition.awaitUntil(deadline), span);
  }

  @Override
  public void signal() {
    SpanContext parent = extract(spanContextMap);
    Span span = TracingHelper.buildSpan("signal", parent, traceWithActiveSpanOnly);
    span.setTag("condition", name);
    decorateAction(condition::signal, span);
  }

  @Override
  public void signalAll() {
    SpanContext parent = extract(spanContextMap);
    Span span = TracingHelper.buildSpan("signalAll", parent, traceWithActiveSpanOnly);
    span.setTag("condition", name);
    decorateAction(condition::signalAll, span);
  }

}
