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

import com.hazelcast.core.ICountDownLatch;
import io.opentracing.Span;
import java.util.concurrent.TimeUnit;

public class TracingCountDownLatch implements ICountDownLatch {

  private final ICountDownLatch latch;
  private final TracingHelper helper;

  public TracingCountDownLatch(ICountDownLatch latch, boolean traceWithActiveSpanOnly) {
    this.latch = latch;
    helper = new TracingHelper(traceWithActiveSpanOnly);
  }

  @Override
  public boolean await(long timeout, TimeUnit unit) throws InterruptedException {
    Span span = helper.buildSpan("await", latch);
    span.setTag("timeout", timeout);
    span.setTag("unit", nullable(unit));
    return decorateExceptionally(() -> latch.await(timeout, unit), span);
  }

  @Override
  public void countDown() {
    Span span = helper.buildSpan("countDown", latch);
    decorateAction(latch::countDown, span);
  }

  @Override
  public int getCount() {
    Span span = helper.buildSpan("getCount", latch);
    return decorate(latch::getCount, span);
  }

  @Override
  public boolean trySetCount(int count) {
    Span span = helper.buildSpan("trySetCount", latch);
    span.setTag("count", count);
    return decorate(() -> latch.trySetCount(count), span);
  }

  @Override
  public String getPartitionKey() {
    return latch.getPartitionKey();
  }

  @Override
  public String getName() {
    return latch.getName();
  }

  @Override
  public String getServiceName() {
    return latch.getServiceName();
  }

  @Override
  public void destroy() {
    Span span = helper.buildSpan("destroy", latch);
    decorateAction(latch::destroy, span);
  }


}
