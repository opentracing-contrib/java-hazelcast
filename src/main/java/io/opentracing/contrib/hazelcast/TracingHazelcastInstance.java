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
import static io.opentracing.contrib.hazelcast.TracingHelper.nullable;
import static io.opentracing.contrib.hazelcast.TracingHelper.nullableClass;

import com.hazelcast.cardinality.CardinalityEstimator;
import com.hazelcast.config.Config;
import com.hazelcast.core.ClientService;
import com.hazelcast.core.Cluster;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.DistributedObjectListener;
import com.hazelcast.core.Endpoint;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.core.IAtomicReference;
import com.hazelcast.core.ICacheManager;
import com.hazelcast.core.ICountDownLatch;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.core.IList;
import com.hazelcast.core.ILock;
import com.hazelcast.core.IMap;
import com.hazelcast.core.IQueue;
import com.hazelcast.core.ISemaphore;
import com.hazelcast.core.ISet;
import com.hazelcast.core.ITopic;
import com.hazelcast.core.IdGenerator;
import com.hazelcast.core.LifecycleService;
import com.hazelcast.core.MultiMap;
import com.hazelcast.core.PartitionService;
import com.hazelcast.core.ReplicatedMap;
import com.hazelcast.cp.CPSubsystem;
import com.hazelcast.crdt.pncounter.PNCounter;
import com.hazelcast.durableexecutor.DurableExecutorService;
import com.hazelcast.flakeidgen.FlakeIdGenerator;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.quorum.QuorumService;
import com.hazelcast.ringbuffer.Ringbuffer;
import com.hazelcast.scheduledexecutor.IScheduledExecutorService;
import com.hazelcast.transaction.HazelcastXAResource;
import com.hazelcast.transaction.TransactionContext;
import com.hazelcast.transaction.TransactionException;
import com.hazelcast.transaction.TransactionOptions;
import com.hazelcast.transaction.TransactionalTask;
import io.opentracing.Span;
import java.util.Collection;
import java.util.concurrent.ConcurrentMap;

public class TracingHazelcastInstance implements HazelcastInstance {

  private final HazelcastInstance instance;
  private final boolean traceWithActiveSpanOnly;
  private final TracingHelper helper;

  public TracingHazelcastInstance(HazelcastInstance instance,
      boolean traceWithActiveSpanOnly) {
    this.instance = instance;
    this.traceWithActiveSpanOnly = traceWithActiveSpanOnly;
    helper = new TracingHelper(traceWithActiveSpanOnly);
  }

  @Override
  public String getName() {
    return instance.getName();
  }

  @Override
  public <E> IQueue<E> getQueue(String s) {
    return new TracingQueue<>(instance.getQueue(s), traceWithActiveSpanOnly);
  }

  @Override
  public <E> ITopic<E> getTopic(String topic) {
    return new TracingTopic<>(topic, false, instance, traceWithActiveSpanOnly);
  }

  @Override
  public <E> ISet<E> getSet(String s) {
    return new TracingSet<>(instance.getSet(s), traceWithActiveSpanOnly);
  }

  @Override
  public <E> IList<E> getList(String s) {
    return new TracingList<>(instance.getList(s), traceWithActiveSpanOnly);
  }

  @Override
  public <K, V> IMap<K, V> getMap(String s) {
    return new TracingMap<>(instance.getMap(s), traceWithActiveSpanOnly);
  }

  @Override
  public <K, V> ReplicatedMap<K, V> getReplicatedMap(String s) {
    return new TracingReplicatedMap<>(instance.getReplicatedMap(s),
        traceWithActiveSpanOnly);
  }

  @Deprecated
  @Override
  public JobTracker getJobTracker(String s) {
    return instance.getJobTracker(s);
  }

  @Override
  public <K, V> MultiMap<K, V> getMultiMap(String s) {
    return new TracingMultiMap<>(instance.getMultiMap(s), traceWithActiveSpanOnly);
  }

  @Override
  public ILock getLock(String s) {
    return new TracingLock(instance.getLock(s), traceWithActiveSpanOnly);
  }

  @Override
  public <E> Ringbuffer<E> getRingbuffer(String s) {
    return new TracingRingbuffer<>(instance.getRingbuffer(s), traceWithActiveSpanOnly);
  }

  @Override
  public <E> ITopic<E> getReliableTopic(String s) {
    return new TracingTopic<>(s, true, instance, traceWithActiveSpanOnly);
  }

  @Override
  public Cluster getCluster() {
    return instance.getCluster();
  }

  @Override
  public Endpoint getLocalEndpoint() {
    return instance.getLocalEndpoint();
  }

  @Override
  public IExecutorService getExecutorService(String s) {
    return new TracingExecutorService(instance.getExecutorService(s),
        traceWithActiveSpanOnly);
  }

  @Override
  public DurableExecutorService getDurableExecutorService(
      String s) {
    return instance.getDurableExecutorService(s);
  }

  @Override
  public <T> T executeTransaction(TransactionalTask<T> transactionalTask)
      throws TransactionException {
    Span span = helper.buildSpan("executeTransaction");
    span.setTag("transactionalTask", nullableClass(transactionalTask));
    return decorate(() -> instance.executeTransaction(transactionalTask), span);
  }

  @Override
  public <T> T executeTransaction(TransactionOptions transactionOptions,
      TransactionalTask<T> transactionalTask) throws TransactionException {
    Span span = helper.buildSpan("executeTransaction");
    span.setTag("transactionOptions", nullable(transactionOptions));
    span.setTag("transactionalTask", nullableClass(transactionalTask));
    return decorate(() -> instance.executeTransaction(transactionOptions, transactionalTask), span);
  }

  @Override
  public TransactionContext newTransactionContext() {
    return instance.newTransactionContext();
  }

  @Override
  public TransactionContext newTransactionContext(
      TransactionOptions transactionOptions) {
    return instance.newTransactionContext(transactionOptions);
  }

  @Override
  public IdGenerator getIdGenerator(String s) {
    return instance.getIdGenerator(s);
  }

  @Override
  public FlakeIdGenerator getFlakeIdGenerator(String name) {
    return instance.getFlakeIdGenerator(name);
  }

  @Override
  public IAtomicLong getAtomicLong(String s) {
    return new TracingAtomicLong(instance.getAtomicLong(s), traceWithActiveSpanOnly);
  }

  @Override
  public <E> IAtomicReference<E> getAtomicReference(String s) {
    return new TracingAtomicReference<>(instance.getAtomicReference(s), traceWithActiveSpanOnly);
  }

  @Override
  public ICountDownLatch getCountDownLatch(String s) {
    return new TracingCountDownLatch(instance.getCountDownLatch(s), traceWithActiveSpanOnly);
  }

  @Override
  public ISemaphore getSemaphore(String s) {
    return new TracingSemaphore(instance.getSemaphore(s), traceWithActiveSpanOnly);
  }

  @Override
  public Collection<DistributedObject> getDistributedObjects() {
    return instance.getDistributedObjects();
  }

  @Override
  public String addDistributedObjectListener(
      DistributedObjectListener distributedObjectListener) {
    return instance.addDistributedObjectListener(distributedObjectListener);
  }

  @Override
  public boolean removeDistributedObjectListener(String s) {
    return instance.removeDistributedObjectListener(s);
  }

  @Override
  public Config getConfig() {
    return instance.getConfig();
  }

  @Override
  public PartitionService getPartitionService() {
    return instance.getPartitionService();
  }

  @Override
  public QuorumService getQuorumService() {
    return instance.getQuorumService();
  }

  @Override
  public ClientService getClientService() {

    return instance.getClientService();
  }

  @Override
  public LoggingService getLoggingService() {

    return instance.getLoggingService();
  }

  @Override
  public LifecycleService getLifecycleService() {
    return instance.getLifecycleService();
  }

  @Override
  public <T extends DistributedObject> T getDistributedObject(String serviceName,
      String name) {
    return instance.getDistributedObject(serviceName, name);
  }

  @Override
  public ConcurrentMap<String, Object> getUserContext() {
    return instance.getUserContext();
  }

  @Override
  public HazelcastXAResource getXAResource() {
    return instance.getXAResource();
  }

  @Override
  public ICacheManager getCacheManager() {
    return instance.getCacheManager();
  }

  @Override
  public CardinalityEstimator getCardinalityEstimator(String s) {
    return instance.getCardinalityEstimator(s);
  }

  @Override
  public PNCounter getPNCounter(String name) {
    return instance.getPNCounter(name);
  }

  @Override
  public IScheduledExecutorService getScheduledExecutorService(String s) {
    return instance.getScheduledExecutorService(s);
  }

  @Override
  public CPSubsystem getCPSubsystem() {
    return new TracingCPSubsystem(instance.getCPSubsystem(), traceWithActiveSpanOnly);
  }

  @Override
  public void shutdown() {
    instance.shutdown();
  }
}
