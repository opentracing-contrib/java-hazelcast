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
import static io.opentracing.contrib.hazelcast.TracingHelper.nullable;

import com.hazelcast.core.ISemaphore;
import io.opentracing.Span;
import java.util.concurrent.TimeUnit;

public class TracingSemaphore implements ISemaphore {

  private final ISemaphore semaphore;
  private final TracingHelper helper;

  public TracingSemaphore(ISemaphore semaphore, boolean traceWithActiveSpanOnly) {
    this.semaphore = semaphore;
    helper = new TracingHelper(traceWithActiveSpanOnly);
  }

  @Override
  public String getName() {
    return semaphore.getName();
  }

  @Override
  public boolean init(int permits) {
    Span span = helper.buildSpan("init", semaphore);
    span.setTag("permits", permits);
    return decorate(() -> semaphore.init(permits), span);
  }

  @Override
  public void acquire() throws InterruptedException {
    Span span = helper.buildSpan("acquire", semaphore);
    decorateActionExceptionally(semaphore::acquire, span);
  }

  @Override
  public void acquire(int permits) throws InterruptedException {
    Span span = helper.buildSpan("acquire", semaphore);
    span.setTag("permits", permits);
    decorateActionExceptionally(() -> semaphore.acquire(permits), span);
  }

  @Override
  public int availablePermits() {
    Span span = helper.buildSpan("availablePermits", semaphore);
    return decorate(semaphore::availablePermits, span);
  }

  @Override
  public int drainPermits() {
    Span span = helper.buildSpan("drainPermits", semaphore);
    return decorate(semaphore::drainPermits, span);
  }

  @Override
  public void reducePermits(int reduction) {
    Span span = helper.buildSpan("reducePermits", semaphore);
    span.setTag("reduction", reduction);
    decorateAction(() -> semaphore.reducePermits(reduction), span);
  }

  @Override
  public void increasePermits(int increase) {
    Span span = helper.buildSpan("increasePermits", semaphore);
    span.setTag("increase", increase);
    decorateAction(() -> semaphore.increasePermits(increase), span);
  }

  @Override
  public void release() {
    Span span = helper.buildSpan("release", semaphore);
    decorateAction(semaphore::release, span);
  }

  @Override
  public void release(int permits) {
    Span span = helper.buildSpan("release", semaphore);
    span.setTag("permits", permits);
    decorateAction(() -> semaphore.release(permits), span);
  }

  @Override
  public boolean tryAcquire() {
    Span span = helper.buildSpan("tryAcquire", semaphore);
    return decorate(semaphore::tryAcquire, span);
  }

  @Override
  public boolean tryAcquire(int permits) {
    Span span = helper.buildSpan("tryAcquire", semaphore);
    span.setTag("permits", permits);
    return decorate(() -> semaphore.tryAcquire(permits), span);
  }

  @Override
  public boolean tryAcquire(long timeout, TimeUnit unit) throws InterruptedException {
    Span span = helper.buildSpan("tryAcquire", semaphore);
    span.setTag("timeout", timeout);
    span.setTag("unit", nullable(unit));
    return decorateExceptionally(() -> semaphore.tryAcquire(timeout, unit), span);
  }

  @Override
  public boolean tryAcquire(int permits, long timeout, TimeUnit unit) throws InterruptedException {
    Span span = helper.buildSpan("tryAcquire", semaphore);
    span.setTag("permits", permits);
    span.setTag("timeout", timeout);
    span.setTag("unit", nullable(unit));
    return decorateExceptionally(() -> semaphore.tryAcquire(permits, timeout, unit), span);
  }

  @Override
  public String getPartitionKey() {
    return semaphore.getPartitionKey();
  }

  @Override
  public String getServiceName() {
    return semaphore.getServiceName();
  }

  @Override
  public void destroy() {
    Span span = helper.buildSpan("destroy", semaphore);
    decorateAction(semaphore::destroy, span);
  }

}
