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
import static io.opentracing.contrib.hazelcast.TracingHelper.decorateActionExceptionally;
import static io.opentracing.contrib.hazelcast.TracingHelper.decorateExceptionally;
import static io.opentracing.contrib.hazelcast.TracingHelper.inject;
import static io.opentracing.contrib.hazelcast.TracingHelper.nullable;

import com.hazelcast.core.ICondition;
import com.hazelcast.core.ILock;
import io.opentracing.Span;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;

public class TracingLock implements ILock {

  private final ILock lock;
  private final TracingHelper helper;
  private final boolean traceWithActiveSpanOnly;

  public TracingLock(ILock lock, boolean traceWithActiveSpanOnly) {
    this.lock = lock;
    this.traceWithActiveSpanOnly = traceWithActiveSpanOnly;
    helper = new TracingHelper(traceWithActiveSpanOnly);
  }

  @Override
  @Deprecated
  public Object getKey() {
    Span span = helper.buildSpan("getKey", lock);
    return decorate(lock::getKey, span);
  }

  @Override
  public void lock() {
    Span span = helper.buildSpan("lock", lock);
    decorateAction(lock::lock, span);
  }

  @Override
  public boolean tryLock() {
    Span span = helper.buildSpan("tryLock", lock);
    return decorate(lock::tryLock, span);
  }

  @Override
  public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
    Span span = helper.buildSpan("tryLock", lock);
    span.setTag("time", time);
    span.setTag("unit", nullable(unit));
    return decorateExceptionally(() -> lock.tryLock(time, unit), span);
  }

  @Override
  public boolean tryLock(long time, TimeUnit unit, long leaseTime,
      TimeUnit leaseUnit) throws InterruptedException {
    Span span = helper.buildSpan("tryLock", lock);
    span.setTag("time", time);
    span.setTag("unit", nullable(unit));
    span.setTag("leaseTime", leaseTime);
    span.setTag("leaseUnit", nullable(leaseUnit));
    return decorateExceptionally(() -> lock.tryLock(time, unit, leaseTime, leaseUnit), span);
  }

  @Override
  public void unlock() {
    Span span = helper.buildSpan("unlock", lock);
    decorateAction(lock::unlock, span);
  }

  @Override
  public void lock(long leaseTime, TimeUnit timeUnit) {
    Span span = helper.buildSpan("lock", lock);
    span.setTag("leaseTime", leaseTime);
    span.setTag("timeUnit", nullable(timeUnit));
    decorateAction(() -> lock.lock(leaseTime, timeUnit), span);
  }

  @Override
  public void forceUnlock() {
    Span span = helper.buildSpan("forceUnlock", lock);
    decorateAction(lock::forceUnlock, span);
  }

  @Override
  public Condition newCondition() {
    // Unsupported
    return lock.newCondition();
  }

  @Override
  public ICondition newCondition(String name) {
    Span span = helper.buildSpan("newCondition", lock);
    span.setTag("conditionName", name);
    return decorate(
        () -> new TracingCondition(lock.newCondition(name), name, traceWithActiveSpanOnly,
            inject(span)), span);
  }

  @Override
  public boolean isLocked() {
    Span span = helper.buildSpan("isLocked", lock);
    return decorate(lock::isLocked, span);
  }

  @Override
  public boolean isLockedByCurrentThread() {
    Span span = helper.buildSpan("isLockedByCurrentThread", lock);
    return decorate(lock::isLockedByCurrentThread, span);
  }

  @Override
  public int getLockCount() {
    Span span = helper.buildSpan("getLockCount", lock);
    return decorate(lock::getLockCount, span);
  }

  @Override
  public long getRemainingLeaseTime() {
    Span span = helper.buildSpan("getRemainingLeaseTime", lock);
    return decorate(lock::getRemainingLeaseTime, span);
  }

  @Override
  public void lockInterruptibly() throws InterruptedException {
    Span span = helper.buildSpan("lockInterruptibly", lock);
    decorateActionExceptionally(lock::lockInterruptibly, span);
  }

  @Override
  public String getPartitionKey() {
    return lock.getPartitionKey();
  }

  @Override
  public String getName() {
    return lock.getName();
  }

  @Override
  public String getServiceName() {
    return lock.getServiceName();
  }

  @Override
  public void destroy() {
    Span span = helper.buildSpan("destroy", lock);
    decorateAction(lock::destroy, span);
  }

}
