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
import static io.opentracing.contrib.hazelcast.TracingHelper.mapToString;
import static io.opentracing.contrib.hazelcast.TracingHelper.nullable;
import static io.opentracing.contrib.hazelcast.TracingHelper.nullableClass;

import com.hazelcast.core.Member;
import com.hazelcast.core.MultiExecutionCallback;
import io.opentracing.Span;
import io.opentracing.SpanContext;
import java.util.Map;

public class TracingMultiExecutionCallback implements MultiExecutionCallback {

  private final MultiExecutionCallback callback;
  private final boolean traceWithActiveSpanOnly;
  private final Map<String, String> spanContextMap;

  public TracingMultiExecutionCallback(MultiExecutionCallback callback,
      boolean traceWithActiveSpanOnly, Map<String, String> spanContextMap) {
    this.callback = callback;
    this.traceWithActiveSpanOnly = traceWithActiveSpanOnly;
    this.spanContextMap = spanContextMap;
  }

  @Override
  public void onResponse(Member member, Object value) {
    SpanContext parent = extract(spanContextMap);
    Span span = TracingHelper.buildSpan("onResponse", parent, traceWithActiveSpanOnly);
    span.setTag("member", member.getAddress().toString());
    span.setTag("value", nullable(value));
    span.setTag("callback", nullableClass(callback));
    decorateAction(() -> callback.onResponse(member, value), span);
  }

  @Override
  public void onComplete(Map<Member, Object> values) {
    SpanContext parent = extract(spanContextMap);
    Span span = TracingHelper.buildSpan("onComplete", parent, traceWithActiveSpanOnly);
    span.setTag("callback", nullableClass(callback));
    span.setTag("values", mapToString(values));
    decorateAction(() -> callback.onComplete(values), span);
  }
}
