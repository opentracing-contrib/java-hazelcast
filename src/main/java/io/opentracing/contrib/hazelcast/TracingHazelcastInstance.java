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
import com.hazelcast.durableexecutor.DurableExecutorService;
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
import java.util.Collection;
import java.util.concurrent.ConcurrentMap;

public class TracingHazelcastInstance implements HazelcastInstance {

  private final HazelcastInstance instance;
  private final boolean traceWithActiveSpanOnly;

  public TracingHazelcastInstance(HazelcastInstance instance,
      boolean traceWithActiveSpanOnly) {
    this.instance = instance;
    this.traceWithActiveSpanOnly = traceWithActiveSpanOnly;
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
    //TODO?
    return instance.getCluster();
  }

  @Override
  public Endpoint getLocalEndpoint() {
    // TODO?
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
    // TODO: after beta
    return instance.getDurableExecutorService(s);
  }

  @Override
  public <T> T executeTransaction(TransactionalTask<T> transactionalTask)
      throws TransactionException {
    // TODO
    return instance.executeTransaction(transactionalTask);
  }

  @Override
  public <T> T executeTransaction(TransactionOptions transactionOptions,
      TransactionalTask<T> transactionalTask) throws TransactionException {
    // TODO
    return instance.executeTransaction(transactionOptions, transactionalTask);
  }

  @Override
  public TransactionContext newTransactionContext() {
    // TODO?
    return instance.newTransactionContext();
  }

  @Override
  public TransactionContext newTransactionContext(
      TransactionOptions transactionOptions) {
    // TODO?
    return instance.newTransactionContext(transactionOptions);
  }

  @Override
  public IdGenerator getIdGenerator(String s) {
    // TODO?
    return instance.getIdGenerator(s);
  }

  @Override
  public IAtomicLong getAtomicLong(String s) {
    return new TracingAtomicLong(instance.getAtomicLong(s), traceWithActiveSpanOnly);
  }

  @Override
  public <E> IAtomicReference<E> getAtomicReference(String s) {
    // TODO
    return instance.getAtomicReference(s);
  }

  @Override
  public ICountDownLatch getCountDownLatch(String s) {
    //TODO
    return instance.getCountDownLatch(s);
  }

  @Override
  public ISemaphore getSemaphore(String s) {
    // TODO
    return instance.getSemaphore(s);
  }

  @Override
  public Collection<DistributedObject> getDistributedObjects() {
    // TODO?
    return instance.getDistributedObjects();
  }

  @Override
  public String addDistributedObjectListener(
      DistributedObjectListener distributedObjectListener) {
    // TODO?
    return instance.addDistributedObjectListener(distributedObjectListener);
  }

  @Override
  public boolean removeDistributedObjectListener(String s) {
    // TODO?
    return instance.removeDistributedObjectListener(s);
  }

  @Override
  public Config getConfig() {
    return instance.getConfig();
  }

  @Override
  public PartitionService getPartitionService() {
    //TODO?
    return instance.getPartitionService();
  }

  @Override
  public QuorumService getQuorumService() {
    //TODO?
    return instance.getQuorumService();
  }

  @Override
  public ClientService getClientService() {
    //TODO?
    return instance.getClientService();
  }

  @Override
  public LoggingService getLoggingService() {
    //TODO?
    return instance.getLoggingService();
  }

  @Override
  public LifecycleService getLifecycleService() {
    //TODO?
    return instance.getLifecycleService();
  }

  @Override
  public <T extends DistributedObject> T getDistributedObject(String s,
      String s1) {
    //TODO?
    return instance.getDistributedObject(s, s1);
  }

  @Override
  public ConcurrentMap<String, Object> getUserContext() {
    //TODO?
    return instance.getUserContext();
  }

  @Override
  public HazelcastXAResource getXAResource() {
    //TODO?
    return instance.getXAResource();
  }

  @Override
  public ICacheManager getCacheManager() {
    //TODO?
    return instance.getCacheManager();
  }

  @Override
  public CardinalityEstimator getCardinalityEstimator(String s) {
    //TODO?
    return instance.getCardinalityEstimator(s);
  }

  @Override
  public IScheduledExecutorService getScheduledExecutorService(String s) {
    // TODO
    return instance.getScheduledExecutorService(s);
  }

  @Override
  public void shutdown() {
    instance.shutdown();
  }
}
