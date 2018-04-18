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
import static io.opentracing.contrib.hazelcast.TracingHelper.mapToString;
import static io.opentracing.contrib.hazelcast.TracingHelper.nullable;
import static io.opentracing.contrib.hazelcast.TracingHelper.nullableClass;

import com.hazelcast.core.EntryListener;
import com.hazelcast.core.ReplicatedMap;
import com.hazelcast.monitor.LocalReplicatedMapStats;
import com.hazelcast.query.Predicate;
import io.opentracing.Span;
import java.util.Collection;
import java.util.Comparator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

public class TracingReplicatedMap<K, V> implements ReplicatedMap<K, V> {

  private final ReplicatedMap<K, V> map;
  private final TracingHelper helper;

  public TracingReplicatedMap(ReplicatedMap<K, V> map,
      boolean traceWithActiveSpanOnly) {
    this.map = map;
    helper = new TracingHelper(traceWithActiveSpanOnly);
  }

  @Override
  public V put(K key, V value, long ttl, TimeUnit timeUnit) {
    Span span = helper.buildSpan("put", map);
    span.setTag("key", nullable(key));
    span.setTag("value", nullable(value));
    span.setTag("ttl", ttl);
    span.setTag("timeUnit", nullable(timeUnit));
    return decorate(() -> map.put(key, value, ttl, timeUnit), span);
  }

  @Override
  public void clear() {
    Span span = helper.buildSpan("clear", map);
    decorateAction(map::clear, span);
  }

  @Override
  public boolean removeEntryListener(String id) {
    Span span = helper.buildSpan("removeEntryListener", map);
    span.setTag("id", nullable(id));
    return decorate(() -> map.removeEntryListener(id), span);
  }

  @Override
  public String addEntryListener(EntryListener<K, V> listener) {
    Span span = helper.buildSpan("addEntryListener", map);
    span.setTag("listener", nullableClass(listener));
    return decorate(() -> map.addEntryListener(listener), span);
  }

  @Override
  public String addEntryListener(EntryListener<K, V> listener, K key) {
    Span span = helper.buildSpan("addEntryListener", map);
    span.setTag("listener", nullableClass(listener));
    span.setTag("key", nullable(key));
    return decorate(() -> map.addEntryListener(listener, key), span);
  }

  @Override
  public String addEntryListener(EntryListener<K, V> listener,
      Predicate<K, V> predicate) {
    Span span = helper.buildSpan("addEntryListener", map);
    span.setTag("listener", nullableClass(listener));
    span.setTag("predicate", nullable(predicate));
    return decorate(() -> map.addEntryListener(listener, predicate), span);
  }

  @Override
  public String addEntryListener(EntryListener<K, V> listener,
      Predicate<K, V> predicate, K key) {
    Span span = helper.buildSpan("addEntryListener", map);
    span.setTag("listener", nullableClass(listener));
    span.setTag("predicate", nullable(predicate));
    span.setTag("key", nullable(key));
    return decorate(() -> map.addEntryListener(listener, predicate, key), span);
  }

  @Override
  public Collection<V> values() {
    Span span = helper.buildSpan("values", map);
    return decorate(map::values, span);
  }

  @Override
  public Collection<V> values(Comparator<V> comparator) {
    Span span = helper.buildSpan("values", map);
    span.setTag("comparator", nullableClass(comparator));
    return decorate(() -> map.values(comparator), span);
  }

  @Override
  public Set<Entry<K, V>> entrySet() {
    Span span = helper.buildSpan("entrySet", map);
    return decorate(map::entrySet, span);
  }

  @Override
  public Set<K> keySet() {
    Span span = helper.buildSpan("keySet", map);
    return decorate(map::keySet, span);
  }

  @Override
  public LocalReplicatedMapStats getReplicatedMapStats() {
    Span span = helper.buildSpan("getReplicatedMapStats", map);
    return decorate(map::getReplicatedMapStats, span);
  }

  @Override
  public int size() {
    Span span = helper.buildSpan("size", map);
    return decorate(map::size, span);
  }

  @Override
  public boolean isEmpty() {
    Span span = helper.buildSpan("isEmpty", map);
    return decorate(map::isEmpty, span);
  }

  @Override
  public boolean containsKey(Object key) {
    Span span = helper.buildSpan("containsKey", map);
    span.setTag("key", nullable(key));
    return decorate(() -> map.containsKey(key), span);
  }

  @Override
  public boolean containsValue(Object value) {
    Span span = helper.buildSpan("containsValue", map);
    span.setTag("value", nullable(value));
    return decorate(() -> map.containsValue(value), span);
  }

  @Override
  public V get(Object key) {
    Span span = helper.buildSpan("get", map);
    span.setTag("key", nullable(key));
    return decorate(() -> map.get(key), span);
  }

  @Override
  public V put(K key, V value) {
    Span span = helper.buildSpan("put", map);
    span.setTag("key", nullable(key));
    span.setTag("value", nullable(value));
    return decorate(() -> map.put(key, value), span);
  }

  @Override
  public V remove(Object key) {
    Span span = helper.buildSpan("remove", map);
    span.setTag("key", nullable(key));
    return decorate(() -> map.remove(key), span);
  }

  @Override
  public void putAll(Map<? extends K, ? extends V> m) {
    Span span = helper.buildSpan("putAll", map);
    span.setTag("mappings", mapToString(m));
    decorateAction(() -> map.putAll(m), span);
  }

  @Override
  public boolean equals(Object o) {
    return map.equals(o);
  }

  @Override
  public int hashCode() {
    return map.hashCode();
  }

  @Override
  public V getOrDefault(Object key, V defaultValue) {
    Span span = helper.buildSpan("getOrDefault", map);
    span.setTag("key", nullable(key));
    span.setTag("defaultValue", nullable(defaultValue));
    return decorate(() -> map.getOrDefault(key, defaultValue), span);
  }

  @Override
  public void forEach(BiConsumer<? super K, ? super V> action) {
    Span span = helper.buildSpan("forEach", map);
    span.setTag("action", nullableClass(action));
    decorateAction(() -> map.forEach(action), span);
  }

  @Override
  public void replaceAll(BiFunction<? super K, ? super V, ? extends V> function) {
    Span span = helper.buildSpan("replaceAll", map);
    span.setTag("function", nullableClass(function));
    decorateAction(() -> map.replaceAll(function), span);
  }

  @Override
  public V putIfAbsent(K key, V value) {
    Span span = helper.buildSpan("putIfAbsent", map);
    span.setTag("key", nullable(key));
    span.setTag("value", nullable(value));
    return decorate(() -> map.putIfAbsent(key, value), span);
  }

  @Override
  public boolean remove(Object key, Object value) {
    Span span = helper.buildSpan("remove", map);
    span.setTag("key", nullable(key));
    span.setTag("value", nullable(value));
    return decorate(() -> map.remove(key, value), span);
  }

  @Override
  public boolean replace(K key, V oldValue, V newValue) {
    Span span = helper.buildSpan("replace", map);
    span.setTag("key", nullable(key));
    span.setTag("oldValue", nullable(oldValue));
    span.setTag("newValue", nullable(newValue));
    return decorate(() -> map.replace(key, oldValue, newValue), span);
  }

  @Override
  public V replace(K key, V value) {
    Span span = helper.buildSpan("replace", map);
    span.setTag("key", nullable(key));
    span.setTag("value", nullable(value));
    return decorate(() -> map.replace(key, value), span);
  }

  @Override
  public V computeIfAbsent(K key, Function<? super K, ? extends V> mappingFunction) {
    Span span = helper.buildSpan("computeIfAbsent", map);
    span.setTag("key", nullable(key));
    span.setTag("mappingFunction", nullableClass(mappingFunction));
    return decorate(() -> map.computeIfAbsent(key, mappingFunction), span);
  }

  @Override
  public V computeIfPresent(K key,
      BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
    Span span = helper.buildSpan("computeIfPresent", map);
    span.setTag("key", nullable(key));
    span.setTag("mappingFunction", nullableClass(remappingFunction));
    return decorate(() -> map.computeIfPresent(key, remappingFunction), span);
  }

  @Override
  public V compute(K key,
      BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
    Span span = helper.buildSpan("compute", map);
    span.setTag("key", nullable(key));
    span.setTag("mappingFunction", nullableClass(remappingFunction));
    return decorate(() -> map.compute(key, remappingFunction), span);
  }

  @Override
  public V merge(K key, V value,
      BiFunction<? super V, ? super V, ? extends V> remappingFunction) {
    Span span = helper.buildSpan("merge", map);
    span.setTag("key", nullable(key));
    span.setTag("value", nullable(value));
    span.setTag("mappingFunction", nullableClass(remappingFunction));
    return decorate(() -> map.merge(key, value, remappingFunction), span);
  }

  @Override
  public String getPartitionKey() {
    return map.getPartitionKey();
  }

  @Override
  public String getName() {
    return map.getName();
  }

  @Override
  public String getServiceName() {
    return map.getServiceName();
  }

  @Override
  public void destroy() {
    Span span = helper.buildSpan("destroy", map);
    decorateAction(map::destroy, span);
  }


}
