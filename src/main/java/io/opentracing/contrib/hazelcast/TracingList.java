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

import com.hazelcast.core.IList;
import com.hazelcast.core.ItemListener;
import io.opentracing.Span;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;

public class TracingList<E> implements IList<E> {

  private final IList<E> list;
  private final boolean traceWithActiveSpanOnly;
  private final TracingHelper helper;

  public TracingList(IList<E> list, boolean traceWithActiveSpanOnly) {
    this.list = list;
    this.traceWithActiveSpanOnly = traceWithActiveSpanOnly;
    helper = new TracingHelper(traceWithActiveSpanOnly);
  }

  @Override
  public int size() {
    Span span = helper.buildSpan("size", list);
    return decorate(list::size, span);
  }

  @Override
  public boolean isEmpty() {
    Span span = helper.buildSpan("isEmpty", list);
    return decorate(list::isEmpty, span);
  }

  @Override
  public boolean contains(Object element) {
    Span span = helper.buildSpan("contains", list);
    span.setTag("element", nullable(element));
    return decorate(() -> list.contains(element), span);
  }

  @Override
  public Iterator<E> iterator() {
    Span span = helper.buildSpan("iterator", list);
    return decorate(list::iterator, span);
  }

  @Override
  public Object[] toArray() {
    Span span = helper.buildSpan("toArray", list);
    return decorate(list::toArray, span);
  }

  @Override
  public <T> T[] toArray(T[] a) {
    Span span = helper.buildSpan("toArray", list);
    return decorate(() -> list.toArray(a), span);
  }

  @Override
  public boolean add(E element) {
    Span span = helper.buildSpan("add", list);
    span.setTag("element", nullable(element));
    return decorate(() -> list.add(element), span);
  }

  @Override
  public boolean remove(Object element) {
    Span span = helper.buildSpan("remove", list);
    span.setTag("element", nullable(element));
    return decorate(() -> list.remove(element), span);
  }

  @Override
  public boolean containsAll(Collection<?> collection) {
    Span span = helper.buildSpan("containsAll", list);
    span.setTag("collection", nullable(collection));
    return decorate(() -> list.containsAll(collection), span);
  }

  @Override
  public boolean addAll(Collection<? extends E> collection) {
    Span span = helper.buildSpan("addAll", list);
    span.setTag("collection", nullable(collection));
    return decorate(() -> list.addAll(collection), span);
  }

  @Override
  public boolean addAll(int index, Collection<? extends E> collection) {
    Span span = helper.buildSpan("addAll", list);
    span.setTag("index", index);
    span.setTag("collection", nullable(collection));
    return decorate(() -> list.addAll(index, collection), span);
  }

  @Override
  public boolean removeAll(Collection<?> collection) {
    Span span = helper.buildSpan("removeAll", list);
    span.setTag("collection", nullable(collection));
    return decorate(() -> list.removeAll(collection), span);
  }

  @Override
  public boolean retainAll(Collection<?> collection) {
    Span span = helper.buildSpan("retainAll", list);
    span.setTag("collection", nullable(collection));
    return decorate(() -> list.retainAll(collection), span);
  }

  @Override
  public void replaceAll(UnaryOperator<E> operator) {
    Span span = helper.buildSpan("replaceAll", list);
    span.setTag("operator", nullableClass(operator));
    decorateAction(() -> list.replaceAll(operator), span);
  }

  @Override
  public void sort(Comparator<? super E> c) {
    Span span = helper.buildSpan("clear", list);
    span.setTag("comparator", nullableClass(c));
    decorateAction(() -> list.sort(c), span);
  }

  @Override
  public void clear() {
    Span span = helper.buildSpan("clear", list);
    decorateAction(list::clear, span);
  }

  @Override
  public boolean equals(Object o) {
    return list.equals(o);
  }

  @Override
  public int hashCode() {
    return list.hashCode();
  }

  @Override
  public E get(int index) {
    Span span = helper.buildSpan("get", list);
    span.setTag("index", index);
    return decorate(() -> list.get(index), span);
  }

  @Override
  public E set(int index, E element) {
    Span span = helper.buildSpan("set", list);
    span.setTag("index", index);
    span.setTag("element", nullable(element));
    return decorate(() -> list.set(index, element), span);
  }

  @Override
  public void add(int index, E element) {
    Span span = helper.buildSpan("add", list);
    span.setTag("index", index);
    span.setTag("element", nullable(element));
    decorateAction(() -> list.add(index, element), span);
  }

  @Override
  public E remove(int index) {
    Span span = helper.buildSpan("remove", list);
    span.setTag("index", index);
    return decorate(() -> list.remove(index), span);
  }

  @Override
  public int indexOf(Object element) {
    Span span = helper.buildSpan("indexOf", list);
    span.setTag("element", nullable(element));
    return decorate(() -> list.indexOf(element), span);
  }

  @Override
  public int lastIndexOf(Object element) {
    Span span = helper.buildSpan("lastIndexOf", list);
    span.setTag("element", nullable(element));
    return decorate(() -> list.lastIndexOf(element), span);
  }

  @Override
  public ListIterator<E> listIterator() {
    return list.listIterator();
  }

  @Override
  public ListIterator<E> listIterator(int index) {
    return list.listIterator(index);
  }

  @Override
  public List<E> subList(int fromIndex, int toIndex) {
    Span span = helper.buildSpan("subList", list);
    span.setTag("fromIndex", fromIndex);
    span.setTag("toIndex", toIndex);
    return decorate(() -> list.subList(fromIndex, toIndex), span);
  }

  @Override
  public Spliterator<E> spliterator() {
    return list.spliterator();
  }

  @Override
  public boolean removeIf(Predicate<? super E> filter) {
    Span span = helper.buildSpan("removeIf", list);
    span.setTag("filter", nullable(filter));
    return decorate(() -> list.removeIf(filter), span);
  }

  @Override
  public Stream<E> stream() {
    return list.stream();
  }

  @Override
  public Stream<E> parallelStream() {
    return list.parallelStream();
  }

  @Override
  public void forEach(Consumer<? super E> action) {
    Span span = helper.buildSpan("forEach", list);
    span.setTag("action", nullableClass(action));
    decorateAction(() -> list.forEach(action), span);
  }

  @Override
  public String getName() {
    return list.getName();
  }

  @Override
  public String addItemListener(ItemListener<E> listener, boolean includeValue) {
    Span span = helper.buildSpan("addItemListener", list);
    span.setTag("listener", nullableClass(listener));
    span.setTag("includeValue", includeValue);
    return decorate(() -> list.addItemListener(listener, includeValue), span);
  }

  @Override
  public boolean removeItemListener(String registrationId) {
    Span span = helper.buildSpan("removeItemListener", list);
    span.setTag("registrationId", registrationId);
    return decorate(() -> list.removeItemListener(registrationId), span);
  }

  @Override
  public String getPartitionKey() {
    return list.getPartitionKey();
  }

  @Override
  public String getServiceName() {
    return list.getServiceName();
  }

  @Override
  public void destroy() {
    Span span = helper.buildSpan("destroy", list);
    decorateAction(list::destroy, span);
  }


}
