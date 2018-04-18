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

import static org.awaitility.Awaitility.await;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.hazelcast.config.Config;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.core.ILock;
import com.hazelcast.core.IMap;
import com.hazelcast.core.ITopic;
import com.hazelcast.core.Member;
import com.hazelcast.core.MultiMap;
import com.hazelcast.core.ReplicatedMap;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;
import com.hazelcast.query.SqlPredicate;
import com.hazelcast.ringbuffer.Ringbuffer;
import io.opentracing.mock.MockSpan;
import io.opentracing.mock.MockTracer;
import io.opentracing.tag.Tags;
import io.opentracing.util.GlobalTracer;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TracingTest {

  private static HazelcastInstance hazelcast;
  private static HazelcastInstance hazelcast2;
  private static final MockTracer tracer = new MockTracer();

  @Before
  public void before() {
    tracer.reset();
  }

  @BeforeClass
  public static void init() {
    Config config = new Config();
    hazelcast = new TracingHazelcastInstance(Hazelcast.newHazelcastInstance(config), false);
    hazelcast2 = new TracingHazelcastInstance(Hazelcast.newHazelcastInstance(config), false);
    GlobalTracer.register(tracer);
  }

  @AfterClass
  public static void shutdown() {
    Hazelcast.shutdownAll();
  }

  @Test
  public void testMap() {
    IMap<String, String> map = hazelcast.getMap("map");
    map.put("key", "value");
    assertEquals("value", map.get("key"));

    //Concurrent Map methods
    map.putIfAbsent("somekey", "somevalue");
    map.replace("key", "value", "newvalue");

    assertEquals("newvalue", map.get("key"));

    List<MockSpan> spans = tracer.finishedSpans();
    assertEquals(5, spans.size());
    checkSpans(spans);
    assertNull(tracer.activeSpan());
  }

  @Test
  public void testEntryProcessor() {
    IMap<String, Integer> map = hazelcast.getMap("map");
    map.put("key", 0);
    map.executeOnKey("key", new TestEntryProcessor());
    System.out.println("new value:" + map.get("key"));
    List<MockSpan> spans = tracer.finishedSpans();
    assertEquals(4, spans.size());
    checkSpans(spans);
    assertNull(tracer.activeSpan());
  }

  @Test
  public void testExecutorService() {
    IExecutorService ex = hazelcast.getExecutorService("executor");
    ex.submit(new MessagePrinter("message to any node"));

    Iterator<Member> iterator = hazelcast.getCluster().getMembers().iterator();
    Member firstMember = iterator.next();
    Member secondMember = iterator.next();

    ex.executeOnMember(new MessagePrinter("message to first member"),
        firstMember);
    ex.executeOnMember(new MessagePrinter("message to second member"),
        secondMember);

    ex.executeOnAllMembers(new MessagePrinter("message to all members"));

    ex.executeOnKeyOwner(new MessagePrinter("message to the member that owns the following key"),
        "key");

    await().atMost(15, TimeUnit.SECONDS).until(reportedSpansSize(), equalTo(11));

    List<MockSpan> spans = tracer.finishedSpans();
    assertEquals(11, spans.size());
    checkSpans(spans);
    assertNull(tracer.activeSpan());
  }

  @Test
  public void testQueue() throws Exception {
    BlockingQueue<String> queue = hazelcast.getQueue("queue");
    queue.offer("item");
    assertEquals("item", queue.poll());

    //Timed blocking Operations
    queue.offer("anotheritem", 500, TimeUnit.MILLISECONDS);
    String anotherItem = queue.poll(5, TimeUnit.SECONDS);
    assertEquals("anotheritem", anotherItem);

    //Indefinitely blocking Operations
    queue.put("yetanotheritem");
    assertEquals("yetanotheritem", queue.take());

    List<MockSpan> spans = tracer.finishedSpans();
    assertEquals(6, spans.size());
    checkSpans(spans);
    assertNull(tracer.activeSpan());
  }

  @Test
  public void testQuery() {
    IMap<String, User> users = hazelcast.getMap("users");

    // Just generate some random users
    User user = new User();
    user.active = true;
    user.age = 19;
    user.username = "user";
    users.put("users", user);

    Predicate sqlQuery = new SqlPredicate("active AND age BETWEEN 18 AND 21)");

    Predicate criteriaQuery = Predicates.and(
        Predicates.equal("active", true),
        Predicates.between("age", 18, 21)
    );

    Collection<User> result1 = users.values(sqlQuery);
    Collection<User> result2 = users.values(criteriaQuery);

    System.out.println(result1);
    System.out.println(result2);

    List<MockSpan> spans = tracer.finishedSpans();
    assertEquals(3, spans.size());
    checkSpans(spans);
    assertNull(tracer.activeSpan());
  }

  @Test
  public void testMultiMap() {
    MultiMap<String, String> multiMap = hazelcast.getMultiMap("multimap");
    multiMap.put("key", "value1");
    multiMap.put("key", "value2");
    multiMap.put("key", "value3");

    Collection<String> values = multiMap.get("key");
    assertTrue(values.contains("value1"));
    assertTrue(values.contains("value2"));
    assertTrue(values.contains("value3"));

    // remove specific key/value pair
    multiMap.remove("key", "value2");

    values = multiMap.get("key");
    assertTrue(values.contains("value1"));
    assertFalse(values.contains("value2"));
    assertTrue(values.contains("value3"));

    List<MockSpan> spans = tracer.finishedSpans();
    assertEquals(6, spans.size());
    checkSpans(spans);
    assertNull(tracer.activeSpan());
  }

  @Test
  public void testTopic() {
    ITopic<String> topic = hazelcast.getTopic("topic");
    topic.addMessageListener(new TestMessageListener());
    topic.publish("Hello to distributed world");
    await().atMost(15, TimeUnit.SECONDS).until(reportedSpansSize(), equalTo(2));

    List<MockSpan> spans = tracer.finishedSpans();
    assertEquals(2, spans.size());
    checkSpans(spans);
    assertNull(tracer.activeSpan());
  }

  @Test
  public void testLock() {
    ILock lock = hazelcast.getLock("lock");
    lock.lock();
    try {
      System.out.println("locked");
    } finally {
      lock.unlock();
    }

    List<MockSpan> spans = tracer.finishedSpans();
    assertEquals(2, spans.size());
    checkSpans(spans);
    assertNull(tracer.activeSpan());
  }

  @Test
  public void testReplicatedMap() {
    ReplicatedMap<String, String> map = hazelcast.getReplicatedMap("replicated-map");
    map.put("key", "value");
    assertEquals("value", map.get("key"));

    List<MockSpan> spans = tracer.finishedSpans();
    assertEquals(2, spans.size());
    checkSpans(spans);
    assertNull(tracer.activeSpan());
  }

  @Test
  public void testAtomicLong() throws ExecutionException, InterruptedException {
    IAtomicLong counter = hazelcast.getAtomicLong("counter");
    assertEquals(3, counter.addAndGet(3));
    counter.alter(new TestMultiplyByTwo());
    assertEquals(6, counter.get());

    counter.addAndGetAsync(4).andThen(new ExecutionCallback<Long>() {
      @Override
      public void onResponse(Long response) {
        System.out.println(response);
      }

      @Override
      public void onFailure(Throwable t) {
        t.printStackTrace();
      }
    });

    counter.alterAsync(new TestMultiplyByTwo());

    await().atMost(15, TimeUnit.SECONDS).until(reportedSpansSize(), equalTo(10));

    List<MockSpan> spans = tracer.finishedSpans();
    assertEquals(10, spans.size());
    checkSpans(spans);
    assertNull(tracer.activeSpan());
  }

  @Test
  public void testRingbuffer() throws Exception {
    final Ringbuffer<Integer> rb = hazelcast.getRingbuffer("rb");

    rb.add(10);

    long seq = rb.tailSequence();
    int value = rb.readOne(seq);
    assertEquals(10, value);

    List<MockSpan> spans = tracer.finishedSpans();
    assertEquals(3, spans.size());
    checkSpans(spans);
    assertNull(tracer.activeSpan());
  }

  private Callable<Integer> reportedSpansSize() {
    return () -> tracer.finishedSpans().size();
  }

  private void checkSpans(List<MockSpan> spans) {
    for (MockSpan span : spans) {
      assertEquals(span.tags().get(Tags.SPAN_KIND.getKey()), Tags.SPAN_KIND_CLIENT);
      assertEquals(TracingHelper.COMPONENT_NAME, span.tags().get(Tags.COMPONENT.getKey()));
      assertEquals(TracingHelper.DB_TYPE, span.tags().get(Tags.DB_TYPE.getKey()));
      assertEquals(0, span.generatedErrors().size());
    }
  }

}
