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
import static io.opentracing.contrib.hazelcast.TracingHelper.decorateExceptionally;
import static io.opentracing.contrib.hazelcast.TracingHelper.inject;
import static io.opentracing.contrib.hazelcast.TracingHelper.nullable;
import static io.opentracing.contrib.hazelcast.TracingHelper.nullableClass;

import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IFunction;
import com.hazelcast.ringbuffer.OverflowPolicy;
import com.hazelcast.ringbuffer.ReadResultSet;
import com.hazelcast.ringbuffer.Ringbuffer;
import io.opentracing.Span;
import java.util.Collection;

public class TracingRingbuffer<E> implements Ringbuffer<E> {

  private final Ringbuffer<E> ringbuffer;
  private final TracingHelper helper;
  private final boolean traceWithActiveSpanOnly;

  public TracingRingbuffer(Ringbuffer<E> ringbuffer,
      boolean traceWithActiveSpanOnly) {
    this.ringbuffer = ringbuffer;
    this.traceWithActiveSpanOnly = traceWithActiveSpanOnly;
    helper = new TracingHelper(traceWithActiveSpanOnly);
  }

  @Override
  public long capacity() {
    Span span = helper.buildSpan("capacity", ringbuffer);
    return decorate(ringbuffer::capacity, span);
  }

  @Override
  public long size() {
    Span span = helper.buildSpan("size", ringbuffer);
    return decorate(ringbuffer::size, span);
  }

  @Override
  public long tailSequence() {
    Span span = helper.buildSpan("tailSequence", ringbuffer);
    return decorate(ringbuffer::tailSequence, span);
  }

  @Override
  public long headSequence() {
    Span span = helper.buildSpan("headSequence", ringbuffer);
    return decorate(ringbuffer::headSequence, span);
  }

  @Override
  public long remainingCapacity() {
    Span span = helper.buildSpan("remainingCapacity", ringbuffer);
    return decorate(ringbuffer::remainingCapacity, span);
  }

  @Override
  public long add(E item) {
    Span span = helper.buildSpan("add", ringbuffer);
    span.setTag("item", nullable(item));
    return decorate(() -> ringbuffer.add(item), span);
  }

  @Override
  public ICompletableFuture<Long> addAsync(E item,
      OverflowPolicy overflowPolicy) {
    Span span = helper.buildSpan("addAsync", ringbuffer);
    span.setTag("item", nullable(item));
    span.setTag("overflowPolicy", nullableClass(overflowPolicy));
    return decorate(() -> new TracingCompletableFuture<>(ringbuffer.addAsync(item, overflowPolicy),
        traceWithActiveSpanOnly, inject(span)), span);
  }

  @Override
  public E readOne(long sequence) throws InterruptedException {
    Span span = helper.buildSpan("readOne", ringbuffer);
    span.setTag("sequence", sequence);
    return decorateExceptionally(() -> ringbuffer.readOne(sequence), span);
  }

  @Override
  public ICompletableFuture<Long> addAllAsync(
      Collection<? extends E> collection,
      OverflowPolicy overflowPolicy) {
    Span span = helper.buildSpan("addAsync", ringbuffer);
    span.setTag("collection", nullable(collection));
    span.setTag("overflowPolicy", nullableClass(overflowPolicy));
    return decorate(
        () -> new TracingCompletableFuture<>(ringbuffer.addAllAsync(collection, overflowPolicy),
            traceWithActiveSpanOnly, inject(span)), span);
  }

  @Override
  public ICompletableFuture<ReadResultSet<E>> readManyAsync(
      long startSequence, int minCount, int maxCount,
      IFunction<E, Boolean> filter) {
    Span span = helper.buildSpan("addAsync", ringbuffer);
    span.setTag("startSequence", startSequence);
    span.setTag("minCount", minCount);
    span.setTag("maxCount", maxCount);
    span.setTag("filter", nullableClass(filter));
    return decorate(() -> new TracingCompletableFuture<>(
        ringbuffer.readManyAsync(startSequence, minCount, maxCount, filter),
        traceWithActiveSpanOnly, inject(span)), span);
  }

  @Override
  public String getPartitionKey() {
    return ringbuffer.getPartitionKey();
  }

  @Override
  public String getName() {
    return ringbuffer.getName();
  }

  @Override
  public String getServiceName() {
    return ringbuffer.getServiceName();
  }

  @Override
  public void destroy() {
    Span span = helper.buildSpan("destroy", ringbuffer);
    decorateAction(ringbuffer::destroy, span);
  }


}
