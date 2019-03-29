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
import static io.opentracing.contrib.hazelcast.TracingHelper.nullable;
import static io.opentracing.contrib.hazelcast.TracingHelper.nullableClass;

import com.hazelcast.core.EntryListener;
import com.hazelcast.core.MultiMap;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.aggregation.Aggregation;
import com.hazelcast.mapreduce.aggregation.Supplier;
import com.hazelcast.monitor.LocalMultiMapStats;
import io.opentracing.Span;
import java.util.Collection;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class TracingMultiMap<K, V> implements MultiMap<K, V> {

  private final MultiMap<K, V> map;
  private final TracingHelper helper;

  public TracingMultiMap(MultiMap<K, V> map, boolean traceWithActiveSpanOnly) {
    this.map = map;
    helper = new TracingHelper(traceWithActiveSpanOnly);
  }

  @Override
  public String getName() {
    return map.getName();
  }

  @Override
  public boolean put(K key, V value) {
    Span span = helper.buildSpan("put", map);
    span.setTag("key", nullable(key));
    span.setTag("value", nullable(value));
    return decorate(() -> map.put(key, value), span);
  }

  @Override
  public Collection<V> get(K key) {
    Span span = helper.buildSpan("get", map);
    span.setTag("key", nullable(key));
    return decorate(() -> map.get(key), span);
  }

  @Override
  public boolean remove(Object key, Object value) {
    Span span = helper.buildSpan("remove", map);
    span.setTag("key", nullable(key));
    span.setTag("value", nullable(value));
    return decorate(() -> map.remove(key, value), span);
  }

  @Override
  public Collection<V> remove(Object key) {
    Span span = helper.buildSpan("remove", map);
    span.setTag("key", nullable(key));
    return decorate(() -> map.remove(key), span);
  }

  @Override
  public void delete(Object key) {
    Span span = helper.buildSpan("delete", map);
    span.setTag("key", nullable(key));
    decorateAction(() -> map.delete(key), span);
  }

  @Override
  public Set<K> localKeySet() {
    Span span = helper.buildSpan("localKeySet", map);
    return decorate(map::localKeySet, span);
  }

  @Override
  public Set<K> keySet() {
    Span span = helper.buildSpan("keySet", map);
    return decorate(map::keySet, span);
  }

  @Override
  public Collection<V> values() {
    Span span = helper.buildSpan("values", map);
    return decorate(map::values, span);
  }

  @Override
  public Set<Entry<K, V>> entrySet() {
    Span span = helper.buildSpan("entrySet", map);
    return decorate(map::entrySet, span);
  }

  @Override
  public boolean containsKey(K key) {
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
  public boolean containsEntry(K key, V value) {
    Span span = helper.buildSpan("containsEntry", map);
    span.setTag("key", nullable(key));
    span.setTag("value", nullable(value));
    return decorate(() -> map.containsEntry(key, value), span);
  }

  @Override
  public int size() {
    Span span = helper.buildSpan("size", map);
    return decorate(map::size, span);
  }

  @Override
  public void clear() {
    Span span = helper.buildSpan("clear", map);
    decorateAction(map::clear, span);
  }

  @Override
  public int valueCount(K key) {
    Span span = helper.buildSpan("valueCount", map);
    span.setTag("key", nullable(key));
    return decorate(() -> map.valueCount(key), span);
  }

  @Override
  public String addLocalEntryListener(EntryListener<K, V> listener) {
    Span span = helper.buildSpan("addLocalEntryListener", map);
    span.setTag("listener", nullableClass(listener));
    return decorate(() -> map.addLocalEntryListener(listener), span);
  }

  @Override
  public String addEntryListener(EntryListener<K, V> listener,
      boolean includeValue) {
    Span span = helper.buildSpan("addEntryListener", map);
    span.setTag("listener", nullableClass(listener));
    span.setTag("includeValue", includeValue);
    return decorate(() -> map.addEntryListener(listener, includeValue), span);
  }

  @Override
  public boolean removeEntryListener(String registrationId) {
    Span span = helper.buildSpan("removeEntryListener", map);
    span.setTag("registrationId", nullable(registrationId));
    return decorate(() -> map.removeEntryListener(registrationId), span);
  }

  @Override
  public String addEntryListener(EntryListener<K, V> listener, K key,
      boolean includeValue) {
    Span span = helper.buildSpan("addEntryListener", map);
    span.setTag("listener", nullableClass(listener));
    span.setTag("key", nullable(key));
    span.setTag("includeValue", includeValue);
    return decorate(() -> map.addEntryListener(listener, key, includeValue), span);
  }

  @Override
  public void lock(K key) {
    Span span = helper.buildSpan("lock", map);
    span.setTag("key", nullable(key));
    decorateAction(() -> map.lock(key), span);
  }

  @Override
  public void lock(K key, long leaseTime, TimeUnit timeUnit) {
    Span span = helper.buildSpan("lock", map);
    span.setTag("key", nullable(key));
    span.setTag("leaseTime", nullable(leaseTime));
    span.setTag("timeUnit", nullable(timeUnit));
    decorateAction(() -> map.lock(key, leaseTime, timeUnit), span);
  }

  @Override
  public boolean isLocked(K key) {
    Span span = helper.buildSpan("isLocked", map);
    span.setTag("key", nullable(key));
    return decorate(() -> map.isLocked(key), span);
  }

  @Override
  public boolean tryLock(K key) {
    Span span = helper.buildSpan("tryLock", map);
    span.setTag("key", nullable(key));
    return decorate(() -> map.tryLock(key), span);
  }

  @Override
  public boolean tryLock(K key, long time, TimeUnit timeUnit) throws InterruptedException {
    Span span = helper.buildSpan("tryLock", map);
    span.setTag("key", nullable(key));
    span.setTag("time", nullable(time));
    span.setTag("timeUnit", nullable(timeUnit));
    return decorateExceptionally(() -> map.tryLock(key, time, timeUnit), span);
  }

  @Override
  public boolean tryLock(K key, long time, TimeUnit timeUnit, long leaseTime,
      TimeUnit leaseTimeUnit) throws InterruptedException {
    Span span = helper.buildSpan("tryLock", map);
    span.setTag("key", nullable(key));
    span.setTag("time", nullable(time));
    span.setTag("timeUnit", nullable(timeUnit));
    span.setTag("leaseTime", nullable(leaseTime));
    span.setTag("leaseTimeUnit", nullable(leaseTimeUnit));
    return decorateExceptionally(() -> map.tryLock(key, time, timeUnit, leaseTime, leaseTimeUnit),
        span);
  }

  @Override
  public void unlock(K key) {
    Span span = helper.buildSpan("unlock", map);
    span.setTag("key", nullable(key));
    decorateAction(() -> map.unlock(key), span);
  }

  @Override
  public void forceUnlock(K key) {
    Span span = helper.buildSpan("forceUnlock", map);
    span.setTag("key", nullable(key));
    decorateAction(() -> map.forceUnlock(key), span);
  }

  @Override
  public LocalMultiMapStats getLocalMultiMapStats() {
    Span span = helper.buildSpan("getLocalMultiMapStats", map);
    return decorate(map::getLocalMultiMapStats, span);
  }

  @Override
  @Deprecated
  public <SuppliedValue, Result> Result aggregate(
      Supplier<K, V, SuppliedValue> supplier,
      Aggregation<K, SuppliedValue, Result> aggregation) {
    Span span = helper.buildSpan("aggregate", map);
    span.setTag("supplier", nullableClass(supplier));
    span.setTag("aggregation", nullableClass(aggregation));
    return decorate(() -> map.aggregate(supplier, aggregation), span);
  }

  @Override
  @Deprecated
  public <SuppliedValue, Result> Result aggregate(
      Supplier<K, V, SuppliedValue> supplier,
      Aggregation<K, SuppliedValue, Result> aggregation,
      JobTracker jobTracker) {
    Span span = helper.buildSpan("aggregate", map);
    span.setTag("supplier", nullableClass(supplier));
    span.setTag("aggregation", nullableClass(aggregation));
    span.setTag("jobTracker", nullableClass(jobTracker));
    return decorate(() -> map.aggregate(supplier, aggregation, jobTracker), span);
  }

  @Override
  public String getPartitionKey() {
    return map.getPartitionKey();
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
