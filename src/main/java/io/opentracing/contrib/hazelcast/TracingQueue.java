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
import static io.opentracing.contrib.hazelcast.TracingHelper.decorateExceptionally;
import static io.opentracing.contrib.hazelcast.TracingHelper.nullable;

import com.hazelcast.core.IQueue;
import com.hazelcast.core.ItemListener;
import com.hazelcast.monitor.LocalQueueStats;
import io.opentracing.Span;
import java.util.Collection;
import java.util.Iterator;
import java.util.Spliterator;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TracingQueue<E> implements IQueue<E> {

  private final IQueue<E> queue;
  private final TracingHelper helper;

  public TracingQueue(IQueue<E> queue, boolean traceWithActiveSpanOnly) {
    this.queue = queue;
    helper = new TracingHelper(traceWithActiveSpanOnly);
  }

  @Override
  public E poll() {
    Span span = helper.buildSpan("poll", queue);
    return decorate(queue::poll, span);
  }

  @Override
  public E poll(long timeout, TimeUnit unit) throws InterruptedException {
    Span span = helper.buildSpan("poll", queue);
    span.setTag("timeout", timeout);
    span.setTag("unit", nullable(unit));
    return decorateExceptionally(() -> queue.poll(timeout, unit), span);
  }

  @Override
  public E take() throws InterruptedException {
    Span span = helper.buildSpan("take", queue);
    return decorateExceptionally(queue::take, span);
  }

  @Override
  public LocalQueueStats getLocalQueueStats() {
    return queue.getLocalQueueStats();
  }

  @Override
  public boolean add(E element) {
    Span span = helper.buildSpan("add", queue);
    span.setTag("element", nullable(element));
    return decorate(() -> queue.add(element), span);
  }

  @Override
  public boolean offer(E element) {
    Span span = helper.buildSpan("offer", queue);
    span.setTag("element", nullable(element));
    return decorate(() -> queue.offer(element), span);
  }

  @Override
  public void put(E element) throws InterruptedException {
    Span span = helper.buildSpan("put", queue);
    span.setTag("element", nullable(element));
    decorateExceptionally(() -> queue.put(element), span);
  }

  @Override
  public boolean offer(E element, long timeout, TimeUnit unit) throws InterruptedException {
    Span span = helper.buildSpan("offer", queue);
    span.setTag("element", nullable(element));
    span.setTag("timeout", timeout);
    span.setTag("unit", nullable(unit));
    return decorateExceptionally(() -> queue.offer(element, timeout, unit), span);
  }

  @Override
  public int remainingCapacity() {
    Span span = helper.buildSpan("remainingCapacity", queue);
    return decorate(queue::remainingCapacity, span);
  }

  @Override
  public boolean remove(Object element) {
    Span span = helper.buildSpan("remove", queue);
    span.setTag("element", nullable(element));
    return decorate(() -> queue.remove(element), span);
  }

  @Override
  public boolean contains(Object element) {
    Span span = helper.buildSpan("contains", queue);
    span.setTag("element", nullable(element));
    return decorate(() -> queue.contains(element), span);
  }

  @Override
  public int drainTo(Collection<? super E> collection) {
    Span span = helper.buildSpan("drainTo", queue);
    span.setTag("collection",
        collection.stream().map(TracingHelper::nullable).collect(Collectors.joining(", ")));
    return decorate(() -> queue.drainTo(collection), span);
  }

  @Override
  public int drainTo(Collection<? super E> collection, int maxElements) {
    Span span = helper.buildSpan("drainTo", queue);
    span.setTag("collection",
        collection.stream().map(TracingHelper::nullable).collect(Collectors.joining(", ")));
    span.setTag("maxElements", nullable(maxElements));
    return decorate(() -> queue.drainTo(collection, maxElements), span);
  }

  @Override
  public E remove() {
    Span span = helper.buildSpan("remove", queue);
    return decorate(queue::remove, span);
  }

  @Override
  public E element() {
    Span span = helper.buildSpan("element", queue);
    return decorate(queue::element, span);
  }

  @Override
  public E peek() {
    Span span = helper.buildSpan("peek", queue);
    return decorate(queue::peek, span);
  }

  @Override
  public int size() {
    Span span = helper.buildSpan("size", queue);
    return decorate(queue::size, span);
  }

  @Override
  public boolean isEmpty() {
    Span span = helper.buildSpan("isEmpty", queue);
    return decorate(queue::isEmpty, span);
  }

  @Override
  public Iterator<E> iterator() {
    // TODO
    return queue.iterator();
  }

  @Override
  public Object[] toArray() {
    Span span = helper.buildSpan("toArray", queue);
    return decorate(queue::toArray, span);
  }

  @Override
  public <T> T[] toArray(T[] a) {
    Span span = helper.buildSpan("toArray", queue);
    return decorate(() -> queue.toArray(a), span);
  }

  @Override
  public boolean containsAll(Collection<?> collection) {
    Span span = helper.buildSpan("containsAll", queue);
    span.setTag("collection", nullable(collection));
    return decorate(() -> queue.containsAll(collection), span);
  }

  @Override
  public boolean addAll(Collection<? extends E> collection) {
    Span span = helper.buildSpan("addAll", queue);
    span.setTag("collection", nullable(collection));
    return decorate(() -> queue.addAll(collection), span);
  }

  @Override
  public boolean removeAll(Collection<?> collection) {
    Span span = helper.buildSpan("removeAll", queue);
    span.setTag("collection", nullable(collection));
    return decorate(() -> queue.removeAll(collection), span);
  }

  @Override
  public boolean removeIf(Predicate<? super E> filter) {
    Span span = helper.buildSpan("removeIf", queue);
    span.setTag("filter", nullable(filter));
    return decorate(()->queue.removeIf(filter), span);
  }

  @Override
  public boolean retainAll(Collection<?> collection) {
    Span span = helper.buildSpan("retainAll", queue);
    span.setTag("collection", nullable(collection));
    return decorate(() -> queue.retainAll(collection), span);
  }

  @Override
  public void clear() {
    Span span = helper.buildSpan("clear", queue);
    decorateAction(queue::clear, span);
  }

  @Override
  public boolean equals(Object element) {
    return queue.equals(element);
  }

  @Override
  public int hashCode() {
    return queue.hashCode();
  }

  @Override
  public Spliterator<E> spliterator() {
    return queue.spliterator();
  }

  @Override
  public Stream<E> stream() {
    return queue.stream();
  }

  @Override
  public Stream<E> parallelStream() {
    return queue.parallelStream();
  }

  @Override
  public void forEach(Consumer<? super E> action) {
    queue.forEach(action);
  }

  @Override
  public String getPartitionKey() {
    return queue.getPartitionKey();
  }

  @Override
  public String getName() {
    return queue.getName();
  }

  @Override
  public String getServiceName() {
    return queue.getServiceName();
  }

  @Override
  public void destroy() {
    queue.destroy();
  }

  @Override
  public String addItemListener(ItemListener<E> listener, boolean includeValue) {
    return queue.addItemListener(listener, includeValue);
  }

  @Override
  public boolean removeItemListener(String registrationId) {
    return queue.removeItemListener(registrationId);
  }


}
