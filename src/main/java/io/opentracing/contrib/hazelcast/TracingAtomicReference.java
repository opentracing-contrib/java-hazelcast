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
import static io.opentracing.contrib.hazelcast.TracingHelper.decorateAction;
import static io.opentracing.contrib.hazelcast.TracingHelper.inject;
import static io.opentracing.contrib.hazelcast.TracingHelper.nullable;
import static io.opentracing.contrib.hazelcast.TracingHelper.nullableClass;

import com.hazelcast.core.IAtomicReference;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IFunction;
import io.opentracing.Span;
import java.util.Map;

public class TracingAtomicReference<E> implements IAtomicReference<E> {

  private final IAtomicReference<E> reference;
  private final TracingHelper helper;
  private final boolean traceWithActiveSpanOnly;

  public TracingAtomicReference(IAtomicReference<E> reference, boolean traceWithActiveSpanOnly) {
    this.reference = reference;
    this.traceWithActiveSpanOnly = traceWithActiveSpanOnly;
    helper = new TracingHelper(traceWithActiveSpanOnly);
  }

  @Override
  public boolean compareAndSet(E expect, E update) {
    Span span = helper.buildSpan("compareAndSet", reference);
    span.setTag("expect", nullable(expect));
    span.setTag("update", nullable(update));
    return decorate(() -> reference.compareAndSet(expect, update), span);
  }

  @Override
  public E get() {
    Span span = helper.buildSpan("get", reference);
    return decorate(reference::get, span);
  }

  @Override
  public void set(E newValue) {
    Span span = helper.buildSpan("set", reference);
    span.setTag("newValue", nullable(newValue));
    decorateAction(() -> reference.set(newValue), span);
  }

  @Override
  public E getAndSet(E newValue) {
    Span span = helper.buildSpan("getAndSet", reference);
    span.setTag("newValue", nullable(newValue));
    return decorate(() -> reference.getAndSet(newValue), span);
  }

  @Override
  public E setAndGet(E update) {
    Span span = helper.buildSpan("setAndGet", reference);
    span.setTag("update", nullable(update));
    return decorate(() -> reference.setAndGet(update), span);
  }

  @Override
  public boolean isNull() {
    Span span = helper.buildSpan("isNull", reference);
    return decorate(reference::isNull, span);
  }

  @Override
  public void clear() {
    Span span = helper.buildSpan("clear", reference);
    decorateAction(reference::clear, span);
  }

  @Override
  public boolean contains(E value) {
    Span span = helper.buildSpan("contains", reference);
    span.setTag("value", nullable(value));
    return decorate(() -> reference.contains(value), span);
  }

  @Override
  public void alter(IFunction<E, E> function) {
    Span span = helper.buildSpan("alter", reference);
    span.setTag("function", nullableClass(function));
    decorateAction(() -> reference
        .alter(new TracingFunction<>(function, traceWithActiveSpanOnly, inject(span))), span);
  }

  @Override
  public E alterAndGet(IFunction<E, E> function) {
    Span span = helper.buildSpan("alterAndGet", reference);
    span.setTag("function", nullableClass(function));
    return decorate(() -> reference
        .alterAndGet(new TracingFunction<>(function, traceWithActiveSpanOnly, inject(span))), span);
  }

  @Override
  public E getAndAlter(IFunction<E, E> function) {
    Span span = helper.buildSpan("getAndAlter", reference);
    span.setTag("function", nullableClass(function));
    return decorate(() -> reference
        .getAndAlter(new TracingFunction<>(function, traceWithActiveSpanOnly, inject(span))), span);
  }

  @Override
  public <R> R apply(IFunction<E, R> function) {
    Span span = helper.buildSpan("apply", reference);
    span.setTag("function", nullableClass(function));
    return decorate(() -> reference
        .apply(new TracingFunction<>(function, traceWithActiveSpanOnly, inject(span))), span);
  }

  @Override
  public ICompletableFuture<Boolean> compareAndSetAsync(E expect, E update) {
    Span span = helper.buildSpan("compareAndSetAsync", reference);
    span.setTag("expect", nullable(expect));
    span.setTag("update", nullable(update));
    return decorate(
        () -> new TracingCompletableFuture<>(reference.compareAndSetAsync(expect, update),
            traceWithActiveSpanOnly, inject(span)), span);
  }

  @Override
  public ICompletableFuture<E> getAsync() {
    Span span = helper.buildSpan("getAsync", reference);
    return decorate(
        () -> new TracingCompletableFuture<>(reference.getAsync(),
            traceWithActiveSpanOnly, inject(span)), span);
  }

  @Override
  public ICompletableFuture<Void> setAsync(E newValue) {
    Span span = helper.buildSpan("setAsync", reference);
    span.setTag("newValue", nullable(newValue));
    return decorate(
        () -> new TracingCompletableFuture<>(reference.setAsync(newValue),
            traceWithActiveSpanOnly, inject(span)), span);
  }

  @Override
  public ICompletableFuture<E> getAndSetAsync(E newValue) {
    Span span = helper.buildSpan("getAndSetAsync", reference);
    span.setTag("newValue", nullable(newValue));
    return decorate(
        () -> new TracingCompletableFuture<>(reference.getAndSetAsync(newValue),
            traceWithActiveSpanOnly, inject(span)), span);
  }

  @Override
  public ICompletableFuture<Boolean> isNullAsync() {
    Span span = helper.buildSpan("isNullAsync", reference);
    return decorate(
        () -> new TracingCompletableFuture<>(reference.isNullAsync(),
            traceWithActiveSpanOnly, inject(span)), span);
  }

  @Override
  public ICompletableFuture<Void> clearAsync() {
    Span span = helper.buildSpan("clearAsync", reference);
    return decorate(
        () -> new TracingCompletableFuture<>(reference.clearAsync(),
            traceWithActiveSpanOnly, inject(span)), span);
  }

  @Override
  public ICompletableFuture<Boolean> containsAsync(E expected) {
    Span span = helper.buildSpan("containsAsync", reference);
    span.setTag("expected", nullable(expected));
    return decorate(
        () -> new TracingCompletableFuture<>(reference.containsAsync(expected),
            traceWithActiveSpanOnly, inject(span)), span);
  }

  @Override
  public ICompletableFuture<Void> alterAsync(
      IFunction<E, E> function) {
    Span span = helper.buildSpan("alterAsync", reference);
    span.setTag("function", nullableClass(function));
    Map<String, String> spanContextMap = inject(span);
    ICompletableFuture<Void> future = reference
        .alterAsync(
            new TracingFunction<>(function, traceWithActiveSpanOnly, spanContextMap));
    return decorate(
        () -> new TracingCompletableFuture<>(future, traceWithActiveSpanOnly, inject(span)),
        span);
  }

  @Override
  public ICompletableFuture<E> alterAndGetAsync(
      IFunction<E, E> function) {
    Span span = helper.buildSpan("alterAndGetAsync", reference);
    span.setTag("function", nullableClass(function));
    Map<String, String> spanContextMap = inject(span);
    ICompletableFuture<E> future = reference
        .alterAndGetAsync(new TracingFunction<>(function, traceWithActiveSpanOnly, spanContextMap));
    return decorate(
        () -> new TracingCompletableFuture<>(future, traceWithActiveSpanOnly, inject(span)),
        span);
  }

  @Override
  public ICompletableFuture<E> getAndAlterAsync(
      IFunction<E, E> function) {
    Span span = helper.buildSpan("getAndAlterAsync", reference);
    span.setTag("function", nullableClass(function));
    Map<String, String> spanContextMap = inject(span);
    ICompletableFuture<E> future = reference
        .getAndAlterAsync(new TracingFunction<>(function, traceWithActiveSpanOnly, spanContextMap));
    return decorate(
        () -> new TracingCompletableFuture<>(future, traceWithActiveSpanOnly, inject(span)),
        span);
  }

  @Override
  public <R> ICompletableFuture<R> applyAsync(
      IFunction<E, R> function) {
    Span span = helper.buildSpan("applyAsync", reference);
    span.setTag("function", nullableClass(function));
    Map<String, String> spanContextMap = inject(span);
    ICompletableFuture<R> future = reference
        .applyAsync(new TracingFunction<>(function, traceWithActiveSpanOnly, spanContextMap));
    return decorate(
        () -> new TracingCompletableFuture<>(future, traceWithActiveSpanOnly, inject(span)),
        span);
  }

  @Override
  public String getPartitionKey() {
    return reference.getPartitionKey();
  }

  @Override
  public String getName() {
    return reference.getName();
  }

  @Override
  public String getServiceName() {
    return reference.getServiceName();
  }

  @Override
  public void destroy() {
    Span span = helper.buildSpan("destroy", reference);
    decorateAction(reference::destroy, span);
  }


}
