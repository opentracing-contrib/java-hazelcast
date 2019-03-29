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

import com.hazelcast.core.DistributedObject;
import io.opentracing.Scope;
import io.opentracing.Span;
import io.opentracing.SpanContext;
import io.opentracing.Tracer;
import io.opentracing.Tracer.SpanBuilder;
import io.opentracing.noop.NoopSpan;
import io.opentracing.noop.NoopSpanContext;
import io.opentracing.propagation.Format.Builtin;
import io.opentracing.propagation.TextMapAdapter;
import io.opentracing.tag.Tags;
import io.opentracing.util.GlobalTracer;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import java.util.stream.Collectors;

class TracingHelper {

  static final String COMPONENT_NAME = "java-hazelcast";
  static final String DB_TYPE = "hazelcast";
  private final Tracer tracer;
  private final boolean traceWithActiveSpanOnly;

  TracingHelper(boolean traceWithActiveSpanOnly) {
    this.tracer = GlobalTracer.get();
    this.traceWithActiveSpanOnly = traceWithActiveSpanOnly;
  }

  public static String nullable(Collection<?> collection) {
    if (collection == null) {
      return "";
    }
    return collection.stream().map(TracingHelper::nullable).collect(Collectors.joining(", "));
  }

  public static <K, V> String mapToString(Map<K, V> map) {
    if (map == null) {
      return "";
    }
    return map.entrySet()
        .stream()
        .map(entry -> entry.getKey() + " -> " + entry.getValue())
        .collect(Collectors.joining(", "));
  }

  Span buildSpan(String operationName, DistributedObject object) {
    if (traceWithActiveSpanOnly && tracer.activeSpan() == null) {
      return NoopSpan.INSTANCE;
    } else {
      return builder(operationName, null)
          .withTag("name", object.getName())
          .withTag("serviceName", object.getServiceName())
          .withTag("partitionKey", object.getPartitionKey())
          .start();
    }
  }

  Span buildSpan(String operationName) {
    if (traceWithActiveSpanOnly && tracer.activeSpan() == null) {
      return NoopSpan.INSTANCE;
    } else {
      return builder(operationName, null)
          .start();
    }
  }

  static Span buildSpan(String operationName, SpanContext parent, boolean traceWithActiveSpanOnly) {
    if (parent instanceof NoopSpanContext) {
      return NoopSpan.INSTANCE;
    }
    if (traceWithActiveSpanOnly && GlobalTracer.get().activeSpan() == null && parent == null) {
      return NoopSpan.INSTANCE;
    } else {
      return builder(operationName, parent).start();
    }
  }

  static String nullable(Object object) {
    return object == null ? "null" : object.toString();
  }

  static String nullableClass(Object object) {
    return object == null ? "null" : object.getClass().getName();
  }

  static void onError(Throwable throwable, Span span) {
    Tags.ERROR.set(span, Boolean.TRUE);

    if (throwable != null) {
      span.log(errorLogs(throwable));
    }
  }

  private static Map<String, Object> errorLogs(Throwable throwable) {
    Map<String, Object> errorLogs = new HashMap<>(2);
    errorLogs.put("event", Tags.ERROR.getKey());
    errorLogs.put("error.object", throwable);
    return errorLogs;
  }

  private static SpanBuilder builder(String operationName, SpanContext parent) {
    SpanBuilder builder = GlobalTracer.get().buildSpan(operationName)
        .withTag(Tags.COMPONENT.getKey(), COMPONENT_NAME)
        .withTag(Tags.SPAN_KIND.getKey(), Tags.SPAN_KIND_CLIENT)
        .withTag(Tags.DB_TYPE.getKey(), DB_TYPE);
    if (parent != null) {
      builder.asChildOf(parent);
    }
    return builder;
  }

  static Map<String, String> inject(Span span) {
    Map<String, String> spanContextMap = new HashMap<>();
    if (span instanceof NoopSpan) {
      return spanContextMap;
    }
    GlobalTracer.get()
        .inject(span.context(), Builtin.TEXT_MAP, new TextMapAdapter(spanContextMap));
    return spanContextMap;
  }

  static SpanContext extract(Map<String, String> spanContextMap) {
    if (spanContextMap == null) {
      return null;
    }
    return GlobalTracer.get().extract(Builtin.TEXT_MAP, new TextMapAdapter(spanContextMap));
  }

  static <T> T decorate(Supplier<T> supplier, Span span) {
    try (Scope ignore = GlobalTracer.get().scopeManager().activate(span)) {
      return supplier.get();
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  static <T> T decorateExceptionally(ThrowingSupplier<T> supplier, Span span)
      throws InterruptedException {
    try (Scope ignore = GlobalTracer.get().scopeManager().activate(span)) {
      return supplier.get();
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  static <T> T decorateExceptionally2(ThrowingSupplier2<T> supplier, Span span)
      throws Exception {
    try (Scope ignore = GlobalTracer.get().scopeManager().activate(span)) {
      return supplier.get();
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  static <T> T decorateExceptionally3(ThrowingSupplier3<T> supplier, Span span)
      throws InterruptedException, ExecutionException {
    try (Scope ignore = GlobalTracer.get().scopeManager().activate(span)) {
      return supplier.get();
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  static <T> T decorateExceptionally4(ThrowingSupplier4<T> supplier, Span span)
      throws InterruptedException, ExecutionException, TimeoutException {
    try (Scope ignore = GlobalTracer.get().scopeManager().activate(span)) {
      return supplier.get();
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  static void decorateActionExceptionally(ThrowingAction action, Span span)
      throws InterruptedException {
    try (Scope ignore = GlobalTracer.get().scopeManager().activate(span)) {
      action.execute();
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  static void decorateAction(Action action, Span span) {
    try (Scope ignore = GlobalTracer.get().scopeManager().activate(span)) {
      action.execute();
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }
}
