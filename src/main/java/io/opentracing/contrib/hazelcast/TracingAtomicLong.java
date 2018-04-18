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
import static io.opentracing.contrib.hazelcast.TracingHelper.nullableClass;

import com.hazelcast.core.IAtomicLong;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IFunction;
import io.opentracing.Span;
import java.util.Map;

public class TracingAtomicLong implements IAtomicLong {

  private final IAtomicLong atomicLong;
  private final TracingHelper helper;
  private final boolean traceWithActiveSpanOnly;

  public TracingAtomicLong(IAtomicLong atomicLong, boolean traceWithActiveSpanOnly) {
    this.atomicLong = atomicLong;
    this.traceWithActiveSpanOnly = traceWithActiveSpanOnly;
    helper = new TracingHelper(traceWithActiveSpanOnly);
  }

  @Override
  public String getName() {
    return atomicLong.getName();
  }

  @Override
  public long addAndGet(long delta) {
    Span span = helper.buildSpan("addAndGet", atomicLong);
    span.setTag("delta", delta);
    return decorate(() -> atomicLong.addAndGet(delta), span);
  }

  @Override
  public boolean compareAndSet(long expect, long update) {
    Span span = helper.buildSpan("compareAndSet", atomicLong);
    span.setTag("expect", expect);
    span.setTag("update", update);
    return decorate(() -> atomicLong.compareAndSet(expect, update), span);
  }

  @Override
  public long decrementAndGet() {
    Span span = helper.buildSpan("decrementAndGet", atomicLong);
    return decorate(atomicLong::decrementAndGet, span);
  }

  @Override
  public long get() {
    Span span = helper.buildSpan("get", atomicLong);
    return decorate(atomicLong::get, span);
  }

  @Override
  public long getAndAdd(long delta) {
    Span span = helper.buildSpan("getAndAdd", atomicLong);
    span.setTag("delta", delta);
    return decorate(() -> atomicLong.getAndAdd(delta), span);
  }

  @Override
  public long getAndSet(long newValue) {
    Span span = helper.buildSpan("getAndSet", atomicLong);
    span.setTag("newValue", newValue);
    return decorate(() -> atomicLong.getAndSet(newValue), span);
  }

  @Override
  public long incrementAndGet() {
    Span span = helper.buildSpan("incrementAndGet", atomicLong);
    return decorate(atomicLong::incrementAndGet, span);
  }

  @Override
  public long getAndIncrement() {
    Span span = helper.buildSpan("getAndIncrement", atomicLong);
    return decorate(atomicLong::getAndIncrement, span);
  }

  @Override
  public void set(long newValue) {
    Span span = helper.buildSpan("set", atomicLong);
    span.setTag("newValue", newValue);
    decorateAction(() -> atomicLong.set(newValue), span);
  }

  @Override
  public void alter(IFunction<Long, Long> function) {
    Span span = helper.buildSpan("alter", atomicLong);
    span.setTag("function", nullableClass(function));
    Map<String, String> spanContextMap = inject(span);
    decorateAction(
        () -> atomicLong.alter(new TracingFunction<>(function, traceWithActiveSpanOnly,
            spanContextMap)), span);
  }

  @Override
  public long alterAndGet(IFunction<Long, Long> function) {
    Span span = helper.buildSpan("alterAndGet", atomicLong);
    span.setTag("function", nullableClass(function));
    Map<String, String> spanContextMap = inject(span);
    return decorate(() -> atomicLong
            .alterAndGet(new TracingFunction<>(function, traceWithActiveSpanOnly, spanContextMap)),
        span);
  }

  @Override
  public long getAndAlter(IFunction<Long, Long> function) {
    Span span = helper.buildSpan("getAndAlter", atomicLong);
    span.setTag("function", nullableClass(function));
    Map<String, String> spanContextMap = inject(span);
    return decorate(() -> atomicLong
            .getAndAlter(new TracingFunction<>(function, traceWithActiveSpanOnly, spanContextMap)),
        span);
  }

  @Override
  public <R> R apply(IFunction<Long, R> function) {
    Span span = helper.buildSpan("apply", atomicLong);
    span.setTag("function", nullableClass(function));
    Map<String, String> spanContextMap = inject(span);
    return decorate(() -> atomicLong
        .apply(new TracingFunction<>(function, traceWithActiveSpanOnly, spanContextMap)), span);
  }

  @Override
  public ICompletableFuture<Long> addAndGetAsync(long delta) {
    Span span = helper.buildSpan("addAndGetAsync", atomicLong);
    span.setTag("delta", delta);
    return decorate(
        () -> new TracingCompletableFuture<>(atomicLong.addAndGetAsync(delta),
            traceWithActiveSpanOnly, inject(span)), span);
  }

  @Override
  public ICompletableFuture<Boolean> compareAndSetAsync(long expect, long update) {
    Span span = helper.buildSpan("compareAndSetAsync", atomicLong);
    span.setTag("expect", expect);
    span.setTag("update", update);
    return decorate(
        () -> new TracingCompletableFuture<>(atomicLong.compareAndSetAsync(expect, update),
            traceWithActiveSpanOnly, inject(span)), span);
  }

  @Override
  public ICompletableFuture<Long> decrementAndGetAsync() {
    Span span = helper.buildSpan("decrementAndGetAsync", atomicLong);
    return decorate(
        () -> new TracingCompletableFuture<>(atomicLong.decrementAndGetAsync(),
            traceWithActiveSpanOnly, inject(span)), span);
  }

  @Override
  public ICompletableFuture<Long> getAsync() {
    Span span = helper.buildSpan("getAsync", atomicLong);
    return decorate(
        () -> new TracingCompletableFuture<>(atomicLong.getAsync(),
            traceWithActiveSpanOnly, inject(span)), span);
  }

  @Override
  public ICompletableFuture<Long> getAndAddAsync(long delta) {
    Span span = helper.buildSpan("getAndAddAsync", atomicLong);
    span.setTag("delta", delta);
    return decorate(
        () -> new TracingCompletableFuture<>(atomicLong.getAndAddAsync(delta),
            traceWithActiveSpanOnly, inject(span)), span);
  }

  @Override
  public ICompletableFuture<Long> getAndSetAsync(long newValue) {
    Span span = helper.buildSpan("getAndSetAsync", atomicLong);
    span.setTag("newValue", newValue);
    return decorate(
        () -> new TracingCompletableFuture<>(atomicLong.getAndSetAsync(newValue),
            traceWithActiveSpanOnly, inject(span)), span);
  }

  @Override
  public ICompletableFuture<Long> incrementAndGetAsync() {
    Span span = helper.buildSpan("incrementAndGetAsync", atomicLong);
    return decorate(
        () -> new TracingCompletableFuture<>(atomicLong.incrementAndGetAsync(),
            traceWithActiveSpanOnly, inject(span)), span);
  }

  @Override
  public ICompletableFuture<Long> getAndIncrementAsync() {
    Span span = helper.buildSpan("getAndIncrementAsync", atomicLong);
    return decorate(
        () -> new TracingCompletableFuture<>(atomicLong.getAndIncrementAsync(),
            traceWithActiveSpanOnly, inject(span)), span);
  }

  @Override
  public ICompletableFuture<Void> setAsync(long newValue) {
    Span span = helper.buildSpan("setAsync", atomicLong);
    span.setTag("newValue", newValue);
    return decorate(
        () -> new TracingCompletableFuture<>(atomicLong.setAsync(newValue),
            traceWithActiveSpanOnly, inject(span)), span);
  }

  @Override
  public ICompletableFuture<Void> alterAsync(
      IFunction<Long, Long> function) {
    Span span = helper.buildSpan("alterAsync", atomicLong);
    span.setTag("function", nullableClass(function));
    Map<String, String> spanContextMap = inject(span);
    ICompletableFuture<Void> future = atomicLong
        .alterAsync(
            new TracingFunction<>(function, traceWithActiveSpanOnly, spanContextMap));
    return decorate(
        () -> new TracingCompletableFuture<>(future, traceWithActiveSpanOnly, inject(span)),
        span);
  }

  @Override
  public ICompletableFuture<Long> alterAndGetAsync(
      IFunction<Long, Long> function) {
    Span span = helper.buildSpan("alterAndGetAsync", atomicLong);
    span.setTag("function", nullableClass(function));
    Map<String, String> spanContextMap = inject(span);
    ICompletableFuture<Long> future = atomicLong
        .alterAndGetAsync(new TracingFunction<>(function, traceWithActiveSpanOnly, spanContextMap));
    return decorate(
        () -> new TracingCompletableFuture<>(future, traceWithActiveSpanOnly, inject(span)),
        span);
  }

  @Override
  public ICompletableFuture<Long> getAndAlterAsync(
      IFunction<Long, Long> function) {
    Span span = helper.buildSpan("getAndAlterAsync", atomicLong);
    span.setTag("function", nullableClass(function));
    Map<String, String> spanContextMap = inject(span);
    ICompletableFuture<Long> future = atomicLong
        .getAndAlterAsync(new TracingFunction<>(function, traceWithActiveSpanOnly, spanContextMap));
    return decorate(
        () -> new TracingCompletableFuture<>(future, traceWithActiveSpanOnly, inject(span)),
        span);
  }

  @Override
  public <R> ICompletableFuture<R> applyAsync(
      IFunction<Long, R> function) {
    Span span = helper.buildSpan("applyAsync", atomicLong);
    span.setTag("function", nullableClass(function));
    Map<String, String> spanContextMap = inject(span);
    ICompletableFuture<R> future = atomicLong
        .applyAsync(new TracingFunction<>(function, traceWithActiveSpanOnly, spanContextMap));
    return decorate(
        () -> new TracingCompletableFuture<>(future, traceWithActiveSpanOnly, inject(span)),
        span);

  }

  @Override
  public String getPartitionKey() {
    return atomicLong.getPartitionKey();
  }

  @Override
  public String getServiceName() {
    return atomicLong.getServiceName();
  }

  @Override
  public void destroy() {
    Span span = helper.buildSpan("destroy", atomicLong);
    decorateAction(atomicLong::destroy, span);
  }

}
