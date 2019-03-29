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

import static io.opentracing.contrib.hazelcast.TracingHelper.decorate;
import static io.opentracing.contrib.hazelcast.TracingHelper.extract;
import static io.opentracing.contrib.hazelcast.TracingHelper.nullableClass;

import com.hazelcast.core.IFunction;
import io.opentracing.Span;
import io.opentracing.SpanContext;
import java.util.Map;

public class TracingFunction<T, R> implements IFunction<T, R> {

  private final IFunction<T, R> function;
  private final boolean traceWithActiveSpanOnly;
  private final Map<String, String> spanContextMap;

  public TracingFunction(IFunction<T, R> function, boolean traceWithActiveSpanOnly,
      Map<String, String> spanContextMap) {
    this.function = function;
    this.traceWithActiveSpanOnly = traceWithActiveSpanOnly;
    this.spanContextMap = spanContextMap;
  }

  @Override
  public R apply(T input) {
    SpanContext parent = extract(spanContextMap);
    Span span = TracingHelper.buildSpan("apply", parent, traceWithActiveSpanOnly);
    span.setTag("function", nullableClass(function));
    return decorate(() -> function.apply(input), span);
  }
}
