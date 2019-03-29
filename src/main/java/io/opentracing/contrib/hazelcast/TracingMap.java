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
import static io.opentracing.contrib.hazelcast.TracingHelper.mapToString;
import static io.opentracing.contrib.hazelcast.TracingHelper.nullable;
import static io.opentracing.contrib.hazelcast.TracingHelper.nullableClass;

import com.hazelcast.aggregation.Aggregator;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.EntryView;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IMap;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.MapInterceptor;
import com.hazelcast.map.QueryCache;
import com.hazelcast.map.listener.MapListener;
import com.hazelcast.map.listener.MapPartitionLostListener;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.aggregation.Aggregation;
import com.hazelcast.mapreduce.aggregation.Supplier;
import com.hazelcast.monitor.LocalMapStats;
import com.hazelcast.projection.Projection;
import com.hazelcast.query.Predicate;
import com.hazelcast.spi.annotation.Beta;
import io.opentracing.Span;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

public class TracingMap<K, V> implements IMap<K, V> {

  private final IMap<K, V> map;
  private final TracingHelper helper;
  private final boolean traceWithActiveSpanOnly;

  public TracingMap(IMap<K, V> map, boolean traceWithActiveSpanOnly) {
    this.map = map;
    this.traceWithActiveSpanOnly = traceWithActiveSpanOnly;
    helper = new TracingHelper(traceWithActiveSpanOnly);
  }

  @Override
  public void putAll(Map<? extends K, ? extends V> m) {
    Span span = helper.buildSpan("putAll", map);
    span.setTag("mappings", mapToString(m));
    decorateAction(() -> map.putAll(m), span);
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
  public boolean remove(Object key, Object value) {
    Span span = helper.buildSpan("remove", map);
    span.setTag("key", nullable(key));
    span.setTag("value", nullable(value));
    return decorate(() -> map.remove(key, value), span);
  }

  @Override
  public void removeAll(Predicate<K, V> predicate) {
    Span span = helper.buildSpan("removeAll", map);
    span.setTag("predicate", nullable(predicate));
    decorateAction(() -> map.removeAll(predicate), span);
  }

  @Override
  public void delete(Object key) {
    Span span = helper.buildSpan("delete", map);
    span.setTag("key", nullable(key));
    decorateAction(() -> map.delete(key), span);
  }

  @Override
  public void flush() {
    Span span = helper.buildSpan("flush", map);
    decorateAction(map::flush, span);
  }

  @Override
  public Map<K, V> getAll(Set<K> keys) {
    Span span = helper.buildSpan("getAll", map);
    span.setTag("keys", keys.stream().map(Object::toString).collect(Collectors.joining(", ")));
    return decorate(() -> map.getAll(keys), span);
  }

  @Override
  public void loadAll(boolean replaceExistingValues) {
    Span span = helper.buildSpan("loadAll", map);
    span.setTag("replaceExistingValues", replaceExistingValues);
    decorateAction(() -> map.loadAll(replaceExistingValues), span);
  }

  @Override
  public void loadAll(Set<K> keys, boolean replaceExistingValues) {
    Span span = helper.buildSpan("loadAll", map);
    span.setTag("keys", keys.stream().map(Object::toString).collect(Collectors.joining(", ")));
    span.setTag("replaceExistingValues", replaceExistingValues);
    decorateAction(() -> map.loadAll(keys, replaceExistingValues), span);
  }

  @Override
  public void clear() {
    Span span = helper.buildSpan("clear", map);
    decorateAction(map::clear, span);
  }

  @Override
  public ICompletableFuture<V> getAsync(K key) {
    Span span = helper.buildSpan("getAsync", map);
    span.setTag("key", nullable(key));
    return decorate(() -> new TracingCompletableFuture<>(map.getAsync(key), traceWithActiveSpanOnly,
        inject(span)), span);
  }

  @Override
  public ICompletableFuture<V> putAsync(K key, V value) {
    Span span = helper.buildSpan("putAsync", map);
    span.setTag("key", nullable(key));
    span.setTag("value", nullable(value));
    return decorate(
        () -> new TracingCompletableFuture<>(map.putAsync(key, value), traceWithActiveSpanOnly,
            inject(span)), span);
  }

  @Override
  public ICompletableFuture<V> putAsync(K key, V value, long ttl,
      TimeUnit timeUnit) {
    Span span = helper.buildSpan("putAsync", map);
    span.setTag("key", nullable(key));
    span.setTag("value", nullable(value));
    span.setTag("ttl", ttl);
    span.setTag("timeUnit", nullable(timeUnit));
    return decorate(() -> new TracingCompletableFuture<>(map.putAsync(key, value, ttl, timeUnit),
        traceWithActiveSpanOnly, inject(span)), span);
  }

  @Override
  public ICompletableFuture<V> putAsync(K key, V value, long ttl, TimeUnit ttlUnit, long maxIdle,
      TimeUnit maxIdleUnit) {
    Span span = helper.buildSpan("putAsync", map);
    span.setTag("key", nullable(key));
    span.setTag("value", nullable(value));
    span.setTag("ttl", ttl);
    span.setTag("maxIdle", maxIdle);
    span.setTag("ttlUnit", nullable(ttlUnit));
    span.setTag("maxIdleUnit", nullable(maxIdleUnit));
    return decorate(() -> new TracingCompletableFuture<>(map.putAsync(key, value, ttl, ttlUnit,
        maxIdle, maxIdleUnit), traceWithActiveSpanOnly, inject(span)), span);
  }

  @Override
  public ICompletableFuture<Void> setAsync(K key, V value) {
    Span span = helper.buildSpan("setAsync", map);
    span.setTag("key", nullable(key));
    span.setTag("value", nullable(value));
    return decorate(() -> new TracingCompletableFuture<>(map.setAsync(key, value),
        traceWithActiveSpanOnly, inject(span)), span);
  }

  @Override
  public ICompletableFuture<Void> setAsync(K key, V value, long ttl,
      TimeUnit timeUnit) {
    Span span = helper.buildSpan("setAsync", map);
    span.setTag("key", nullable(key));
    span.setTag("value", nullable(value));
    span.setTag("ttl", ttl);
    span.setTag("timeUnit", nullable(timeUnit));
    return decorate(() -> new TracingCompletableFuture<>(map.setAsync(key, value, ttl, timeUnit),
        traceWithActiveSpanOnly, inject(span)), span);
  }

  @Override
  public ICompletableFuture<Void> setAsync(K key, V value, long ttl, TimeUnit ttlUnit, long maxIdle,
      TimeUnit maxIdleUnit) {
    Span span = helper.buildSpan("setAsync", map);
    span.setTag("key", nullable(key));
    span.setTag("value", nullable(value));
    span.setTag("ttl", ttl);
    span.setTag("maxIdle", maxIdle);
    span.setTag("ttlUnit", nullable(ttlUnit));
    span.setTag("maxIdleUnit", nullable(maxIdleUnit));
    return decorate(() -> new TracingCompletableFuture<>(map.setAsync(key, value, ttl, ttlUnit,
        maxIdle, maxIdleUnit), traceWithActiveSpanOnly, inject(span)), span);
  }

  @Override
  public ICompletableFuture<V> removeAsync(K key) {
    Span span = helper.buildSpan("removeAsync", map);
    span.setTag("key", nullable(key));
    return decorate(() -> new TracingCompletableFuture<>(map.removeAsync(key),
        traceWithActiveSpanOnly, inject(span)), span);
  }

  @Override
  public boolean tryRemove(K key, long timeout, TimeUnit timeUnit) {
    Span span = helper.buildSpan("tryRemove", map);
    span.setTag("key", nullable(key));
    span.setTag("timeout", timeout);
    span.setTag("timeUnit", nullable(timeUnit));
    return decorate(() -> map.tryRemove(key, timeout, timeUnit), span);
  }

  @Override
  public boolean tryPut(K key, V value, long timeout, TimeUnit timeUnit) {
    Span span = helper.buildSpan("tryPut", map);
    span.setTag("key", nullable(key));
    span.setTag("value", nullable(value));
    span.setTag("timeout", timeout);
    span.setTag("timeUnit", nullable(timeUnit));
    return decorate(() -> map.tryPut(key, value, timeout, timeUnit), span);
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
  public V put(K key, V value, long ttl, TimeUnit ttlUnit, long maxIdle, TimeUnit maxIdleUnit) {
    Span span = helper.buildSpan("put", map);
    span.setTag("key", nullable(key));
    span.setTag("value", nullable(value));
    span.setTag("ttl", ttl);
    span.setTag("maxIdle", maxIdle);
    span.setTag("ttlUnit", nullable(ttlUnit));
    span.setTag("maxIdleUnit", nullable(maxIdleUnit));
    return decorate(() -> map.put(key, value, ttl, ttlUnit, maxIdle, maxIdleUnit), span);
  }

  @Override
  public void putTransient(K key, V value, long ttl, TimeUnit timeUnit) {
    Span span = helper.buildSpan("putTransient", map);
    span.setTag("key", nullable(key));
    span.setTag("value", nullable(value));
    span.setTag("ttl", ttl);
    span.setTag("timeUnit", nullable(timeUnit));
    decorateAction(() -> map.putTransient(key, value, ttl, timeUnit), span);
  }

  @Override
  public void putTransient(K key, V value, long ttl, TimeUnit ttlUnit, long maxIdle,
      TimeUnit maxIdleUnit) {
    Span span = helper.buildSpan("putTransient", map);
    span.setTag("key", nullable(key));
    span.setTag("value", nullable(value));
    span.setTag("ttl", ttl);
    span.setTag("maxIdle", maxIdle);
    span.setTag("ttlUnit", nullable(ttlUnit));
    span.setTag("maxIdleUnit", nullable(maxIdleUnit));
    decorateAction(() -> map.putTransient(key, value, ttl, ttlUnit, maxIdle, maxIdleUnit), span);
  }

  @Override
  public V putIfAbsent(K key, V value) {
    Span span = helper.buildSpan("putIfAbsent", map);
    span.setTag("key", nullable(key));
    span.setTag("value", nullable(value));
    return decorate(() -> map.putIfAbsent(key, value), span);
  }

  @Override
  public V putIfAbsent(K key, V value, long ttl, TimeUnit timeUnit) {
    Span span = helper.buildSpan("putIfAbsent", map);
    span.setTag("key", nullable(key));
    span.setTag("value", nullable(value));
    span.setTag("ttl", ttl);
    span.setTag("timeUnit", nullable(timeUnit));
    return decorate(() -> map.putIfAbsent(key, value, ttl, timeUnit), span);
  }

  @Override
  public V putIfAbsent(K key, V value, long ttl, TimeUnit ttlUnit, long maxIdle,
      TimeUnit maxIdleUnit) {
    Span span = helper.buildSpan("putIfAbsent", map);
    span.setTag("key", nullable(key));
    span.setTag("value", nullable(value));
    span.setTag("ttl", ttl);
    span.setTag("ttlUnit", nullable(ttlUnit));
    span.setTag("maxIdle", maxIdle);
    span.setTag("maxIdleUnit", nullable(maxIdleUnit));
    return decorate(() -> map.putIfAbsent(key, value, ttl, ttlUnit, maxIdle, maxIdleUnit), span);
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
  public void set(K key, V value) {
    Span span = helper.buildSpan("set", map);
    span.setTag("key", nullable(key));
    span.setTag("value", nullable(value));
    decorateAction(() -> map.set(key, value), span);
  }

  @Override
  public void set(K key, V value, long ttl, TimeUnit timeUnit) {
    Span span = helper.buildSpan("set", map);
    span.setTag("key", nullable(key));
    span.setTag("value", nullable(value));
    span.setTag("ttl", ttl);
    span.setTag("timeUnit", nullable(timeUnit));
    decorateAction(() -> map.set(key, value, ttl, timeUnit), span);
  }

  @Override
  public void set(K key, V value, long ttl, TimeUnit ttlUnit, long maxIdle, TimeUnit maxIdleUnit) {
    Span span = helper.buildSpan("set", map);
    span.setTag("key", nullable(key));
    span.setTag("value", nullable(value));
    span.setTag("ttl", ttl);
    span.setTag("ttlUnit", nullable(ttlUnit));
    span.setTag("maxIdle", maxIdle);
    span.setTag("maxIdleUnit", nullable(maxIdleUnit));
    decorateAction(() -> map.set(key, value, ttl, ttlUnit, maxIdle, maxIdleUnit), span);
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
  public String addLocalEntryListener(MapListener listener) {
    Span span = helper.buildSpan("addLocalEntryListener", map);
    span.setTag("listener", nullableClass(listener));
    return decorate(() -> map.addLocalEntryListener(listener), span);
  }

  @Deprecated
  @Override
  public String addLocalEntryListener(EntryListener listener) {
    Span span = helper.buildSpan("addLocalEntryListener", map);
    span.setTag("listener", nullableClass(listener));
    return decorate(() -> map.addLocalEntryListener(listener), span);
  }

  @Override
  public String addLocalEntryListener(MapListener listener,
      Predicate<K, V> predicate, boolean includeValue) {
    Span span = helper.buildSpan("addLocalEntryListener", map);
    span.setTag("listener", nullableClass(listener));
    span.setTag("predicate", nullable(predicate));
    span.setTag("includeValue", includeValue);
    return decorate(() -> map.addLocalEntryListener(listener, predicate, includeValue), span);
  }

  @Deprecated
  @Override
  public String addLocalEntryListener(EntryListener listener,
      Predicate<K, V> predicate, boolean includeValue) {
    Span span = helper.buildSpan("addLocalEntryListener", map);
    span.setTag("listener", nullableClass(listener));
    span.setTag("predicate", nullable(predicate));
    span.setTag("includeValue", includeValue);
    return decorate(() -> map.addLocalEntryListener(listener, predicate, includeValue), span);
  }

  @Override
  public String addLocalEntryListener(MapListener listener,
      Predicate<K, V> predicate, K key, boolean includeValue) {
    Span span = helper.buildSpan("addLocalEntryListener", map);
    span.setTag("listener", nullableClass(listener));
    span.setTag("predicate", nullable(predicate));
    span.setTag("key", nullable(key));
    span.setTag("includeValue", includeValue);
    return decorate(() -> map.addLocalEntryListener(listener, predicate, key, includeValue), span);
  }

  @Deprecated
  @Override
  public String addLocalEntryListener(EntryListener listener,
      Predicate<K, V> predicate, K key, boolean includeValue) {
    Span span = helper.buildSpan("addLocalEntryListener", map);
    span.setTag("listener", nullableClass(listener));
    span.setTag("predicate", nullable(predicate));
    span.setTag("key", nullable(key));
    span.setTag("includeValue", includeValue);
    return decorate(() -> map.addLocalEntryListener(listener, predicate, key, includeValue), span);
  }

  @Override
  public String addInterceptor(MapInterceptor interceptor) {
    Span span = helper.buildSpan("addInterceptor", map);
    span.setTag("interceptor", nullableClass(interceptor));
    return decorate(() -> map.addInterceptor(interceptor), span);
  }

  @Override
  public void removeInterceptor(String id) {
    Span span = helper.buildSpan("removeInterceptor", map);
    span.setTag("id", nullable(id));
    decorateAction(() -> map.removeInterceptor(id), span);
  }

  @Override
  public String addEntryListener(MapListener listener,
      boolean includeValue) {
    Span span = helper.buildSpan("addEntryListener", map);
    span.setTag("listener", nullableClass(listener));
    span.setTag("includeValue", includeValue);
    return decorate(() -> map.addEntryListener(listener, includeValue), span);
  }

  @Deprecated
  @Override
  public String addEntryListener(EntryListener listener, boolean includeValue) {
    Span span = helper.buildSpan("addEntryListener", map);
    span.setTag("listener", nullableClass(listener));
    span.setTag("includeValue", includeValue);
    return decorate(() -> map.addEntryListener(listener, includeValue), span);
  }

  @Override
  public boolean removeEntryListener(String id) {
    Span span = helper.buildSpan("removeEntryListener", map);
    span.setTag("id", nullable(id));
    return decorate(() -> map.removeEntryListener(id), span);
  }

  @Override
  public String addPartitionLostListener(MapPartitionLostListener listener) {
    Span span = helper.buildSpan("addPartitionLostListener", map);
    span.setTag("listener", nullableClass(listener));
    return decorate(() -> map.addPartitionLostListener(listener), span);
  }

  @Override
  public boolean removePartitionLostListener(String id) {
    Span span = helper.buildSpan("removePartitionLostListener", map);
    span.setTag("id", nullable(id));
    return decorate(() -> map.removePartitionLostListener(id), span);
  }

  @Override
  public String addEntryListener(MapListener listener, K key,
      boolean includeValue) {
    Span span = helper.buildSpan("addEntryListener", map);
    span.setTag("listener", nullableClass(listener));
    span.setTag("key", nullable(key));
    span.setTag("includeValue", includeValue);
    return decorate(() -> map.addEntryListener(listener, key, includeValue), span);
  }

  @Deprecated
  @Override
  public String addEntryListener(EntryListener listener, K key,
      boolean includeValue) {
    Span span = helper.buildSpan("addEntryListener", map);
    span.setTag("listener", nullableClass(listener));
    span.setTag("key", nullable(key));
    span.setTag("includeValue", includeValue);
    return decorate(() -> map.addEntryListener(listener, key, includeValue), span);
  }

  @Override
  public String addEntryListener(MapListener listener,
      Predicate<K, V> predicate, boolean includeValue) {
    Span span = helper.buildSpan("addEntryListener", map);
    span.setTag("listener", nullableClass(listener));
    span.setTag("predicate", nullable(predicate));
    span.setTag("includeValue", includeValue);
    return decorate(() -> map.addEntryListener(listener, predicate, includeValue), span);
  }

  @Deprecated
  @Override
  public String addEntryListener(EntryListener listener,
      Predicate<K, V> predicate, boolean includeValue) {
    Span span = helper.buildSpan("addEntryListener", map);
    span.setTag("listener", nullableClass(listener));
    span.setTag("predicate", nullable(predicate));
    span.setTag("includeValue", includeValue);
    return decorate(() -> map.addEntryListener(listener, predicate, includeValue), span);
  }

  @Override
  public String addEntryListener(MapListener listener,
      Predicate<K, V> predicate, K key, boolean includeValue) {
    Span span = helper.buildSpan("addEntryListener", map);
    span.setTag("listener", nullableClass(listener));
    span.setTag("predicate", nullable(predicate));
    span.setTag("key", nullable(key));
    span.setTag("includeValue", includeValue);
    return decorate(() -> map.addEntryListener(listener, predicate, key, includeValue), span);
  }

  @Deprecated
  @Override
  public String addEntryListener(EntryListener listener,
      Predicate<K, V> predicate, K key, boolean includeValue) {
    Span span = helper.buildSpan("addEntryListener", map);
    span.setTag("listener", nullableClass(listener));
    span.setTag("predicate", nullable(predicate));
    span.setTag("key", nullable(key));
    span.setTag("includeValue", includeValue);
    return decorate(() -> map.addEntryListener(listener, predicate, key, includeValue), span);
  }

  @Override
  public EntryView<K, V> getEntryView(K key) {
    Span span = helper.buildSpan("getEntryView", map);
    span.setTag("key", nullable(key));
    return decorate(() -> map.getEntryView(key), span);
  }

  @Override
  public boolean evict(K key) {
    Span span = helper.buildSpan("evict", map);
    span.setTag("key", nullable(key));
    return decorate(() -> map.evict(key), span);
  }

  @Override
  public void evictAll() {
    Span span = helper.buildSpan("evictAll", map);
    decorateAction(map::evictAll, span);
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
  public Set<K> keySet(Predicate predicate) {
    Span span = helper.buildSpan("keySet", map);
    span.setTag("predicate", nullable(predicate));
    return decorate(() -> map.keySet(predicate), span);
  }

  @Override
  public Set<Entry<K, V>> entrySet(Predicate predicate) {
    Span span = helper.buildSpan("entrySet", map);
    span.setTag("predicate", nullable(predicate));
    return decorate(() -> map.entrySet(predicate), span);
  }

  @Override
  public Collection<V> values(Predicate predicate) {
    Span span = helper.buildSpan("values", map);
    span.setTag("predicate", nullable(predicate));
    return decorate(() -> map.values(predicate), span);
  }

  @Override
  public Set<K> localKeySet() {
    Span span = helper.buildSpan("localKeySet", map);
    return decorate(map::localKeySet, span);
  }

  @Override
  public Set<K> localKeySet(Predicate predicate) {
    Span span = helper.buildSpan("localKeySet", map);
    span.setTag("predicate", nullable(predicate));
    return decorate(() -> map.localKeySet(predicate), span);
  }

  @Override
  public void addIndex(String attribute, boolean ordered) {
    Span span = helper.buildSpan("addIndex", map);
    span.setTag("attribute", nullable(attribute));
    span.setTag("ordered", ordered);
    decorateAction(() -> map.addIndex(attribute, ordered), span);
  }

  @Override
  public LocalMapStats getLocalMapStats() {
    Span span = helper.buildSpan("getLocalMapStats", map);
    return decorate(map::getLocalMapStats, span);
  }

  @Override
  @SuppressWarnings("unchecked")
  public Object executeOnKey(K key, EntryProcessor entryProcessor) {
    Span span = helper.buildSpan("executeOnKey", map);
    span.setTag("key", nullable(key));
    span.setTag("entryProcessor", nullableClass(entryProcessor));
    Map<String, String> spanContextMap = inject(span);

    return decorate(() -> map.executeOnKey(key,
        new TracingEntryProcessor<>(entryProcessor, traceWithActiveSpanOnly, spanContextMap)),
        span);
  }

  @Override
  @SuppressWarnings("unchecked")
  public Map<K, Object> executeOnKeys(Set<K> keys,
      EntryProcessor entryProcessor) {
    Span span = helper.buildSpan("executeOnKeys", map);
    span.setTag("keys",
        keys.stream().map(TracingHelper::nullable).collect(Collectors.joining(", ")));
    span.setTag("entryProcessor", nullableClass(entryProcessor));
    return decorate(() -> map.executeOnKeys(keys,
        new TracingEntryProcessor(entryProcessor, traceWithActiveSpanOnly, inject(span))), span);
  }

  @Override
  @SuppressWarnings("unchecked")
  public void submitToKey(K key, EntryProcessor entryProcessor,
      ExecutionCallback callback) {
    Span span = helper.buildSpan("submitToKey", map);
    span.setTag("key", nullable(key));
    span.setTag("entryProcessor", nullableClass(entryProcessor));
    Map<String, String> spanContextMap = inject(span);

    decorateAction(() -> map.submitToKey(key,
        new TracingEntryProcessor(entryProcessor, traceWithActiveSpanOnly, spanContextMap),
        new TracingExecutionCallback(callback, traceWithActiveSpanOnly, spanContextMap)),
        span);
  }

  @Override
  @SuppressWarnings("unchecked")
  public ICompletableFuture submitToKey(K key,
      EntryProcessor entryProcessor) {
    Span span = helper.buildSpan("submitToKey", map);
    span.setTag("key", nullable(key));
    span.setTag("entryProcessor", nullableClass(entryProcessor));
    return decorate(() -> map.submitToKey(key,
        new TracingEntryProcessor(entryProcessor, traceWithActiveSpanOnly, inject(span))), span);
  }

  @Override
  @SuppressWarnings("unchecked")
  public Map<K, Object> executeOnEntries(EntryProcessor entryProcessor) {
    Span span = helper.buildSpan("executeOnEntries", map);
    span.setTag("entryProcessor", nullableClass(entryProcessor));
    return decorate(() -> map.executeOnEntries(
        new TracingEntryProcessor(entryProcessor, traceWithActiveSpanOnly, inject(span))), span);
  }

  @Override
  @SuppressWarnings("unchecked")
  public Map<K, Object> executeOnEntries(EntryProcessor entryProcessor,
      Predicate predicate) {
    Span span = helper.buildSpan("executeOnEntries", map);
    span.setTag("entryProcessor", nullableClass(entryProcessor));
    span.setTag("predicate", nullable(predicate));
    return decorate(() -> map.executeOnEntries(
        new TracingEntryProcessor(entryProcessor, traceWithActiveSpanOnly, inject(span)),
        predicate), span);
  }

  @Override
  public <R> R aggregate(Aggregator<Entry<K, V>, R> aggregator) {
    Span span = helper.buildSpan("aggregate", map);
    span.setTag("aggregator", nullableClass(aggregator));
    return decorate(() -> map.aggregate(aggregator), span);
  }

  @Override
  public <R> R aggregate(Aggregator<Entry<K, V>, R> aggregator,
      Predicate<K, V> predicate) {
    Span span = helper.buildSpan("aggregate", map);
    span.setTag("aggregator", nullableClass(aggregator));
    span.setTag("predicate", nullable(predicate));
    return decorate(() -> map.aggregate(aggregator, predicate), span);
  }

  @Override
  public <R> Collection<R> project(
      Projection<Entry<K, V>, R> projection) {
    Span span = helper.buildSpan("project", map);
    span.setTag("projection", nullableClass(projection));
    return decorate(() -> map.project(projection), span);
  }

  @Override
  public <R> Collection<R> project(
      Projection<Entry<K, V>, R> projection,
      Predicate<K, V> predicate) {
    Span span = helper.buildSpan("project", map);
    span.setTag("projection", nullableClass(projection));
    span.setTag("predicate", nullable(predicate));
    return decorate(() -> map.project(projection, predicate), span);
  }

  @Override
  @Deprecated
  public <SuppliedValue, Result> Result aggregate(
      Supplier<K, V, SuppliedValue> supplier,
      Aggregation<K, SuppliedValue, Result> aggregation) {
    // Ignore
    return map.aggregate(supplier, aggregation);
  }

  @Override
  @Deprecated
  public <SuppliedValue, Result> Result aggregate(
      Supplier<K, V, SuppliedValue> supplier,
      Aggregation<K, SuppliedValue, Result> aggregation,
      JobTracker jobTracker) {
    // Ignore
    return map.aggregate(supplier, aggregation, jobTracker);
  }

  @Override
  @Beta
  public QueryCache<K, V> getQueryCache(String queryCacheName) {
    Span span = helper.buildSpan("getQueryCache", map);
    span.setTag("queryCacheName", nullable(queryCacheName));
    return decorate(() -> map.getQueryCache(queryCacheName), span);
  }

  @Override
  @Beta
  public QueryCache<K, V> getQueryCache(String queryCacheName,
      Predicate<K, V> predicate, boolean includeValue) {
    Span span = helper.buildSpan("getQueryCache", map);
    span.setTag("queryCacheName", nullable(queryCacheName));
    span.setTag("predicate", nullable(predicate));
    span.setTag("includeValue", includeValue);
    return decorate(() -> map.getQueryCache(queryCacheName, predicate, includeValue), span);
  }

  @Override
  @Beta
  public QueryCache<K, V> getQueryCache(String queryCacheName,
      MapListener listener, Predicate<K, V> predicate,
      boolean includeValue) {
    Span span = helper.buildSpan("getQueryCache", map);
    span.setTag("queryCacheName", nullable(queryCacheName));
    span.setTag("predicate", nullable(predicate));
    span.setTag("listener", nullableClass(listener));
    span.setTag("includeValue", includeValue);
    return decorate(() -> map.getQueryCache(queryCacheName, listener, predicate, includeValue),
        span);
  }

  @Override
  public boolean setTtl(K key, long ttl, TimeUnit timeunit) {
    Span span = helper.buildSpan("setTtl", map);
    span.setTag("key", nullable(key));
    span.setTag("ttl", ttl);
    span.setTag("timeunit", nullable(timeunit));
    return decorate(() -> map.setTtl(key, ttl, timeunit), span);
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
  public boolean equals(Object o) {
    return map.equals(o);
  }

  @Override
  public int hashCode() {
    return map.hashCode();
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
