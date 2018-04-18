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
import static io.opentracing.contrib.hazelcast.TracingHelper.nullable;
import static io.opentracing.contrib.hazelcast.TracingHelper.nullableClass;

import com.hazelcast.core.ISet;
import com.hazelcast.core.ItemListener;
import io.opentracing.Span;
import java.util.Collection;
import java.util.Iterator;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Stream;

public class TracingSet<E> implements ISet<E> {

  private final ISet<E> set;
  private final TracingHelper helper;
  private final boolean traceWithActiveSpanOnly;

  public TracingSet(ISet<E> set, boolean traceWithActiveSpanOnly) {
    this.set = set;
    this.traceWithActiveSpanOnly = traceWithActiveSpanOnly;
    helper = new TracingHelper(traceWithActiveSpanOnly);
  }

  @Override
  public int size() {
    Span span = helper.buildSpan("size", set);
    return decorate(set::size, span);
  }

  @Override
  public boolean isEmpty() {
    Span span = helper.buildSpan("isEmpty", set);
    return decorate(set::isEmpty, span);
  }

  @Override
  public boolean contains(Object element) {
    Span span = helper.buildSpan("contains", set);
    span.setTag("element", nullable(element));
    return decorate(() -> set.contains(element), span);
  }

  @Override
  public Iterator<E> iterator() {
    Span span = helper.buildSpan("iterator", set);
    return decorate(set::iterator, span);
  }

  @Override
  public Object[] toArray() {
    Span span = helper.buildSpan("toArray", set);
    return decorate(set::toArray, span);
  }

  @Override
  public <T> T[] toArray(T[] a) {
    Span span = helper.buildSpan("toArray", set);
    return decorate(() -> set.toArray(a), span);
  }

  @Override
  public boolean add(E element) {
    Span span = helper.buildSpan("add", set);
    span.setTag("element", nullable(element));
    return decorate(() -> set.add(element), span);
  }

  @Override
  public boolean remove(Object element) {
    Span span = helper.buildSpan("remove", set);
    span.setTag("element", nullable(element));
    return decorate(() -> set.remove(element), span);
  }

  @Override
  public boolean containsAll(Collection<?> collection) {
    Span span = helper.buildSpan("containsAll", set);
    span.setTag("collection", nullable(collection));
    return decorate(() -> set.containsAll(collection), span);
  }

  @Override
  public boolean addAll(Collection<? extends E> collection) {
    Span span = helper.buildSpan("addAll", set);
    span.setTag("collection", nullable(collection));
    return decorate(() -> set.addAll(collection), span);
  }

  @Override
  public boolean retainAll(Collection<?> collection) {
    Span span = helper.buildSpan("retainAll", set);
    span.setTag("collection", nullable(collection));
    return decorate(() -> set.retainAll(collection), span);
  }

  @Override
  public boolean removeAll(Collection<?> collection) {
    Span span = helper.buildSpan("removeAll", set);
    span.setTag("collection", nullable(collection));
    return decorate(() -> set.removeAll(collection), span);
  }

  @Override
  public void clear() {
    Span span = helper.buildSpan("clear", set);
    decorateAction(set::clear, span);
  }

  @Override
  public boolean equals(Object o) {
    return set.equals(o);
  }

  @Override
  public int hashCode() {
    return set.hashCode();
  }

  @Override
  public Spliterator<E> spliterator() {
    return set.spliterator();
  }

  @Override
  public boolean removeIf(Predicate<? super E> filter) {
    Span span = helper.buildSpan("removeIf", set);
    span.setTag("filter", nullable(filter));
    return decorate(() -> set.removeIf(filter), span);
  }

  @Override
  public Stream<E> stream() {
    return set.stream();
  }

  @Override
  public Stream<E> parallelStream() {
    return set.parallelStream();
  }

  @Override
  public void forEach(Consumer<? super E> action) {
    Span span = helper.buildSpan("forEach", set);
    span.setTag("action", nullableClass(action));
    decorateAction(() -> set.forEach(action), span);
  }

  @Override
  public String getName() {
    return set.getName();
  }

  @Override
  public String addItemListener(ItemListener<E> listener, boolean includeValue) {
    Span span = helper.buildSpan("addItemListener", set);
    span.setTag("listener", nullableClass(listener));
    span.setTag("includeValue", includeValue);
    return decorate(() -> set.addItemListener(listener, includeValue), span);
  }

  @Override
  public boolean removeItemListener(String registrationId) {
    Span span = helper.buildSpan("removeItemListener", set);
    span.setTag("registrationId", registrationId);
    return decorate(() -> set.removeItemListener(registrationId), span);
  }

  @Override
  public String getPartitionKey() {
    return set.getPartitionKey();
  }

  @Override
  public String getServiceName() {
    return set.getServiceName();
  }

  @Override
  public void destroy() {
    Span span = helper.buildSpan("destroy", set);
    decorateAction(set::destroy, span);
  }

}
