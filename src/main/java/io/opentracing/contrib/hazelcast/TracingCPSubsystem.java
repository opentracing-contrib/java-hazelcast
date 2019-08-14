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

import com.hazelcast.core.IAtomicLong;
import com.hazelcast.core.IAtomicReference;
import com.hazelcast.core.ICountDownLatch;
import com.hazelcast.core.ISemaphore;
import com.hazelcast.cp.CPMember;
import com.hazelcast.cp.CPSubsystem;
import com.hazelcast.cp.CPSubsystemManagementService;
import com.hazelcast.cp.lock.FencedLock;
import com.hazelcast.cp.session.CPSessionManagementService;

public class TracingCPSubsystem implements CPSubsystem {
  private final CPSubsystem cpSubsystem;
  private final boolean traceWithActiveSpanOnly;

  public TracingCPSubsystem(CPSubsystem cpSubsystem, boolean traceWithActiveSpanOnly) {
    this.cpSubsystem = cpSubsystem;
    this.traceWithActiveSpanOnly = traceWithActiveSpanOnly;
  }

  @Override
  public IAtomicLong getAtomicLong(String name) {
    return new TracingAtomicLong(cpSubsystem.getAtomicLong(name), traceWithActiveSpanOnly);
  }

  @Override
  public <E> IAtomicReference<E> getAtomicReference(String name) {
    return new TracingAtomicReference<>(cpSubsystem.getAtomicReference(name),
        traceWithActiveSpanOnly);
  }

  @Override
  public ICountDownLatch getCountDownLatch(String name) {
    return new TracingCountDownLatch(cpSubsystem.getCountDownLatch(name), traceWithActiveSpanOnly);
  }

  @Override
  public FencedLock getLock(String name) {
    return cpSubsystem.getLock(name);
  }

  @Override
  public ISemaphore getSemaphore(String name) {
    return new TracingSemaphore(cpSubsystem.getSemaphore(name), traceWithActiveSpanOnly);
  }

  @Override
  public CPMember getLocalCPMember() {
    return cpSubsystem.getLocalCPMember();
  }

  @Override
  public CPSubsystemManagementService getCPSubsystemManagementService() {
    return cpSubsystem.getCPSubsystemManagementService();
  }

  @Override
  public CPSessionManagementService getCPSessionManagementService() {
    return cpSubsystem.getCPSessionManagementService();
  }
}
