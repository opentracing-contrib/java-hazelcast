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
import static io.opentracing.contrib.hazelcast.TracingHelper.decorateExceptionally3;
import static io.opentracing.contrib.hazelcast.TracingHelper.decorateExceptionally4;
import static io.opentracing.contrib.hazelcast.TracingHelper.inject;
import static io.opentracing.contrib.hazelcast.TracingHelper.nullable;
import static io.opentracing.contrib.hazelcast.TracingHelper.nullableClass;

import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.core.Member;
import com.hazelcast.core.MemberSelector;
import com.hazelcast.core.MultiExecutionCallback;
import com.hazelcast.monitor.LocalExecutorStats;
import io.opentracing.Span;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

public class TracingExecutorService implements IExecutorService {

  private final TracingHelper helper;
  private final IExecutorService service;
  private final boolean traceWithActiveSpanOnly;

  public TracingExecutorService(IExecutorService service,
      boolean traceWithActiveSpanOnly) {
    this.service = service;
    this.traceWithActiveSpanOnly = traceWithActiveSpanOnly;
    helper = new TracingHelper(traceWithActiveSpanOnly);
  }

  @Override
  public void execute(Runnable command, MemberSelector memberSelector) {
    Span span = helper.buildSpan("execute", service);
    span.setTag("command", nullableClass(command));
    span.setTag("memberSelector", nullableClass(memberSelector));
    Map<String, String> spanContextMap = inject(span);
    decorateAction(() -> service.execute(
        new TracingRunnable(command, traceWithActiveSpanOnly, spanContextMap), memberSelector),
        span);
  }

  @Override
  public void executeOnKeyOwner(Runnable command, Object key) {
    Span span = helper.buildSpan("executeOnKeyOwner", service);
    span.setTag("command", nullableClass(command));
    span.setTag("key", nullable(key));
    Map<String, String> spanContextMap = inject(span);
    decorateAction(() -> service.executeOnKeyOwner(
        new TracingRunnable(command, traceWithActiveSpanOnly, spanContextMap), key), span);
  }

  @Override
  public void executeOnMember(Runnable command, Member member) {
    Span span = helper.buildSpan("executeOnMember", service);
    span.setTag("command", nullableClass(command));
    span.setTag("member", member.getAddress().toString());
    Map<String, String> spanContextMap = inject(span);
    decorateAction(() -> service.executeOnMember(
        new TracingRunnable(command, traceWithActiveSpanOnly, spanContextMap), member),
        span);
  }

  @Override
  public void executeOnMembers(Runnable command,
      Collection<Member> members) {
    Span span = helper.buildSpan("executeOnMembers", service);
    span.setTag("command", nullableClass(command));
    span.setTag("members",
        members.stream().map(member -> member.getAddress().toString())
            .collect(Collectors.joining(", ")));
    Map<String, String> spanContextMap = inject(span);
    decorateAction(() -> service.executeOnMembers(
        new TracingRunnable(command, traceWithActiveSpanOnly, spanContextMap), members),
        span);
  }

  @Override
  public void executeOnMembers(Runnable command, MemberSelector memberSelector) {
    Span span = helper.buildSpan("executeOnMembers", service);
    span.setTag("command", nullableClass(command));
    span.setTag("memberSelector", nullableClass(memberSelector));
    Map<String, String> spanContextMap = inject(span);
    decorateAction(() -> service.executeOnMembers(
        new TracingRunnable(command, traceWithActiveSpanOnly, spanContextMap), memberSelector),
        span);
  }

  @Override
  public void executeOnAllMembers(Runnable command) {
    Span span = helper.buildSpan("executeOnAllMembers", service);
    span.setTag("command", nullableClass(command));
    Map<String, String> spanContextMap = inject(span);
    decorateAction(() -> service.executeOnAllMembers(
        new TracingRunnable(command, traceWithActiveSpanOnly, spanContextMap)), span);
  }

  @Override
  public <T> Future<T> submit(Callable<T> task,
      MemberSelector memberSelector) {
    Span span = helper.buildSpan("submit", service);
    span.setTag("task", nullableClass(task));
    span.setTag("memberSelector", nullableClass(memberSelector));
    Map<String, String> spanContextMap = inject(span);
    return decorate(() -> service
        .submit(new TracingCallable<>(task, traceWithActiveSpanOnly, spanContextMap),
            memberSelector), span);
  }

  @Override
  public <T> Future<T> submitToKeyOwner(Callable<T> task,
      Object key) {
    Span span = helper.buildSpan("submitToKeyOwner", service);
    span.setTag("task", nullableClass(task));
    span.setTag("key", nullable(key));
    Map<String, String> spanContextMap = inject(span);
    return decorate(() -> service
        .submitToKeyOwner(new TracingCallable<>(task, traceWithActiveSpanOnly, spanContextMap),
            key), span);
  }

  @Override
  public <T> Future<T> submitToMember(Callable<T> task,
      Member member) {
    Span span = helper.buildSpan("submitToMember", service);
    span.setTag("task", nullableClass(task));
    span.setTag("member", member.getAddress().toString());
    Map<String, String> spanContextMap = inject(span);
    return decorate(() -> service
        .submitToMember(new TracingCallable<>(task, traceWithActiveSpanOnly, spanContextMap),
            member), span);
  }

  @Override
  public <T> Map<Member, Future<T>> submitToMembers(
      Callable<T> task, Collection<Member> members) {
    Span span = helper.buildSpan("submitToMembers", service);
    span.setTag("task", nullableClass(task));
    span.setTag("members", members.stream().map(member -> member.getAddress().toString())
        .collect(Collectors.joining(", ")));
    Map<String, String> spanContextMap = inject(span);
    return decorate(() -> service
        .submitToMembers(new TracingCallable<>(task, traceWithActiveSpanOnly, spanContextMap),
            members), span);
  }

  @Override
  public <T> Map<Member, Future<T>> submitToMembers(
      Callable<T> task, MemberSelector memberSelector) {
    Span span = helper.buildSpan("submitToMembers", service);
    span.setTag("task", nullableClass(task));
    span.setTag("memberSelector", nullableClass(memberSelector));
    Map<String, String> spanContextMap = inject(span);
    return decorate(() -> service
        .submitToMembers(new TracingCallable<>(task, traceWithActiveSpanOnly, spanContextMap),
            memberSelector), span);
  }

  @Override
  public <T> Map<Member, Future<T>> submitToAllMembers(
      Callable<T> task) {
    Span span = helper.buildSpan("submitToAllMembers", service);
    span.setTag("task", nullableClass(task));
    Map<String, String> spanContextMap = inject(span);
    return decorate(() -> service
            .submitToAllMembers(new TracingCallable<>(task, traceWithActiveSpanOnly, spanContextMap)),
        span);
  }

  @Override
  public <T> void submit(Runnable task, ExecutionCallback<T> callback) {
    Span span = helper.buildSpan("submit", service);
    span.setTag("task", nullableClass(task));
    span.setTag("callback", nullableClass(callback));
    Map<String, String> spanContextMap = inject(span);
    TracingRunnable tracingRunnable = new TracingRunnable(task, traceWithActiveSpanOnly,
        spanContextMap);
    TracingExecutionCallback<T> tracingExecutionCallback =
        new TracingExecutionCallback<>(callback, traceWithActiveSpanOnly, spanContextMap);
    decorateAction(() -> service.submit(tracingRunnable, tracingExecutionCallback), span);
  }

  @Override
  public <T> void submit(Runnable task, MemberSelector memberSelector,
      ExecutionCallback<T> callback) {
    Span span = helper.buildSpan("submit", service);
    span.setTag("task", nullableClass(task));
    span.setTag("callback", nullableClass(callback));
    span.setTag("memberSelector", nullableClass(memberSelector));
    Map<String, String> spanContextMap = inject(span);
    TracingRunnable tracingRunnable = new TracingRunnable(task, traceWithActiveSpanOnly,
        spanContextMap);
    TracingExecutionCallback<T> tracingExecutionCallback =
        new TracingExecutionCallback<>(callback, traceWithActiveSpanOnly, spanContextMap);
    decorateAction(() -> service.submit(tracingRunnable, memberSelector, tracingExecutionCallback),
        span);
  }

  @Override
  public <T> void submitToKeyOwner(Runnable task, Object key,
      ExecutionCallback<T> callback) {
    Span span = helper.buildSpan("submitToKeyOwner", service);
    span.setTag("task", nullableClass(task));
    span.setTag("callback", nullableClass(callback));
    span.setTag("key", nullable(key));
    Map<String, String> spanContextMap = inject(span);
    TracingRunnable tracingRunnable = new TracingRunnable(task, traceWithActiveSpanOnly,
        spanContextMap);
    TracingExecutionCallback<T> tracingExecutionCallback =
        new TracingExecutionCallback<>(callback, traceWithActiveSpanOnly, spanContextMap);
    decorateAction(() -> service.submitToKeyOwner(tracingRunnable, key, tracingExecutionCallback),
        span);
  }

  @Override
  public <T> void submitToMember(Runnable task, Member member,
      ExecutionCallback<T> callback) {
    Span span = helper.buildSpan("submitToMember", service);
    span.setTag("task", nullableClass(task));
    span.setTag("callback", nullableClass(callback));
    span.setTag("member", member.getAddress().toString());
    Map<String, String> spanContextMap = inject(span);
    TracingRunnable tracingRunnable = new TracingRunnable(task, traceWithActiveSpanOnly,
        spanContextMap);
    TracingExecutionCallback<T> tracingExecutionCallback =
        new TracingExecutionCallback<>(callback, traceWithActiveSpanOnly, spanContextMap);
    decorateAction(() -> service.submitToMember(tracingRunnable, member, tracingExecutionCallback),
        span);
  }

  @Override
  public void submitToMembers(Runnable task, Collection<Member> members,
      MultiExecutionCallback callback) {
    Span span = helper.buildSpan("submitToMembers", service);
    span.setTag("task", nullableClass(task));
    span.setTag("callback", nullableClass(callback));
    span.setTag("members", members.stream().map(member -> member.getAddress().toString())
        .collect(Collectors.joining(", ")));
    Map<String, String> spanContextMap = inject(span);
    TracingRunnable tracingRunnable = new TracingRunnable(task, traceWithActiveSpanOnly,
        spanContextMap);
    TracingMultiExecutionCallback tracingExecutionCallback =
        new TracingMultiExecutionCallback(callback, traceWithActiveSpanOnly, spanContextMap);

    decorateAction(
        () -> service.submitToMembers(tracingRunnable, members, tracingExecutionCallback), span);
  }

  @Override
  public void submitToMembers(Runnable task, MemberSelector memberSelector,
      MultiExecutionCallback callback) {
    Span span = helper.buildSpan("submitToMembers", service);
    span.setTag("task", nullableClass(task));
    span.setTag("callback", nullableClass(callback));
    span.setTag("memberSelector", nullableClass(memberSelector));
    Map<String, String> spanContextMap = inject(span);
    TracingRunnable tracingRunnable = new TracingRunnable(task, traceWithActiveSpanOnly,
        spanContextMap);
    TracingMultiExecutionCallback tracingExecutionCallback =
        new TracingMultiExecutionCallback(callback, traceWithActiveSpanOnly, spanContextMap);

    decorateAction(
        () -> service.submitToMembers(tracingRunnable, memberSelector, tracingExecutionCallback),
        span);
  }

  @Override
  public void submitToAllMembers(Runnable task, MultiExecutionCallback callback) {
    Span span = helper.buildSpan("submitToAllMembers", service);
    span.setTag("task", nullableClass(task));
    span.setTag("callback", nullableClass(callback));
    Map<String, String> spanContextMap = inject(span);
    TracingRunnable tracingRunnable = new TracingRunnable(task, traceWithActiveSpanOnly,
        spanContextMap);
    TracingMultiExecutionCallback tracingExecutionCallback =
        new TracingMultiExecutionCallback(callback, traceWithActiveSpanOnly, spanContextMap);

    decorateAction(
        () -> service.submitToAllMembers(tracingRunnable, tracingExecutionCallback),
        span);
  }

  @Override
  public <T> void submit(Callable<T> task, ExecutionCallback<T> callback) {
    Span span = helper.buildSpan("submit", service);
    span.setTag("task", nullableClass(task));
    span.setTag("callback", nullableClass(callback));
    Map<String, String> spanContextMap = inject(span);
    TracingCallable<T> tracingCallable = new TracingCallable<>(task, traceWithActiveSpanOnly,
        spanContextMap);
    TracingExecutionCallback<T> tracingExecutionCallback =
        new TracingExecutionCallback<>(callback, traceWithActiveSpanOnly, spanContextMap);
    decorateAction(
        () -> service.submit(tracingCallable, tracingExecutionCallback),
        span);
  }

  @Override
  public <T> void submit(Callable<T> task,
      MemberSelector memberSelector,
      ExecutionCallback<T> callback) {
    Span span = helper.buildSpan("submit", service);
    span.setTag("task", nullableClass(task));
    span.setTag("callback", nullableClass(callback));
    span.setTag("memberSelector", nullableClass(memberSelector));
    Map<String, String> spanContextMap = inject(span);
    TracingCallable<T> tracingCallable = new TracingCallable<>(task, traceWithActiveSpanOnly,
        spanContextMap);
    TracingExecutionCallback<T> tracingExecutionCallback =
        new TracingExecutionCallback<>(callback, traceWithActiveSpanOnly, spanContextMap);
    decorateAction(
        () -> service.submit(tracingCallable, memberSelector, tracingExecutionCallback),
        span);
  }

  @Override
  public <T> void submitToKeyOwner(Callable<T> task, Object key,
      ExecutionCallback<T> callback) {
    Span span = helper.buildSpan("submitToKeyOwner", service);
    span.setTag("task", nullableClass(task));
    span.setTag("callback", nullableClass(callback));
    span.setTag("key", nullable(key));
    Map<String, String> spanContextMap = inject(span);
    TracingCallable<T> tracingCallable = new TracingCallable<>(task, traceWithActiveSpanOnly,
        spanContextMap);
    TracingExecutionCallback<T> tracingExecutionCallback =
        new TracingExecutionCallback<>(callback, traceWithActiveSpanOnly, spanContextMap);
    decorateAction(
        () -> service.submitToKeyOwner(tracingCallable, key, tracingExecutionCallback),
        span);
  }

  @Override
  public <T> void submitToMember(Callable<T> task,
      Member member, ExecutionCallback<T> callback) {
    Span span = helper.buildSpan("submitToMember", service);
    span.setTag("task", nullableClass(task));
    span.setTag("callback", nullableClass(callback));
    span.setTag("member", member.getAddress().toString());
    Map<String, String> spanContextMap = inject(span);
    TracingCallable<T> tracingCallable = new TracingCallable<>(task, traceWithActiveSpanOnly,
        spanContextMap);
    TracingExecutionCallback<T> tracingExecutionCallback =
        new TracingExecutionCallback<>(callback, traceWithActiveSpanOnly, spanContextMap);
    decorateAction(
        () -> service.submitToMember(tracingCallable, member, tracingExecutionCallback),
        span);
  }

  @Override
  public <T> void submitToMembers(Callable<T> task,
      Collection<Member> members,
      MultiExecutionCallback callback) {
    Span span = helper.buildSpan("submitToMembers", service);
    span.setTag("task", nullableClass(task));
    span.setTag("callback", nullableClass(callback));
    span.setTag("members", members.stream().map(member -> member.getAddress().toString())
        .collect(Collectors.joining(", ")));
    Map<String, String> spanContextMap = inject(span);
    TracingCallable<T> tracingCallable = new TracingCallable<>(task, traceWithActiveSpanOnly,
        spanContextMap);
    TracingMultiExecutionCallback tracingExecutionCallback =
        new TracingMultiExecutionCallback(callback, traceWithActiveSpanOnly, spanContextMap);
    decorateAction(
        () -> service.submitToMembers(tracingCallable, members, tracingExecutionCallback),
        span);
  }

  @Override
  public <T> void submitToMembers(Callable<T> task,
      MemberSelector memberSelector,
      MultiExecutionCallback callback) {
    Span span = helper.buildSpan("submitToMembers", service);
    span.setTag("task", nullableClass(task));
    span.setTag("callback", nullableClass(callback));
    span.setTag("memberSelector", nullableClass(memberSelector));
    Map<String, String> spanContextMap = inject(span);
    TracingCallable<T> tracingCallable = new TracingCallable<>(task, traceWithActiveSpanOnly,
        spanContextMap);
    TracingMultiExecutionCallback tracingExecutionCallback =
        new TracingMultiExecutionCallback(callback, traceWithActiveSpanOnly, spanContextMap);
    decorateAction(
        () -> service.submitToMembers(tracingCallable, memberSelector, tracingExecutionCallback),
        span);
  }

  @Override
  public <T> void submitToAllMembers(Callable<T> task,
      MultiExecutionCallback callback) {
    Span span = helper.buildSpan("submitToAllMembers", service);
    span.setTag("task", nullableClass(task));
    span.setTag("callback", nullableClass(callback));
    Map<String, String> spanContextMap = inject(span);
    TracingCallable<T> tracingCallable = new TracingCallable<>(task, traceWithActiveSpanOnly,
        spanContextMap);
    TracingMultiExecutionCallback tracingExecutionCallback =
        new TracingMultiExecutionCallback(callback, traceWithActiveSpanOnly, spanContextMap);
    decorateAction(
        () -> service.submitToAllMembers(tracingCallable, tracingExecutionCallback),
        span);
  }

  @Override
  public LocalExecutorStats getLocalExecutorStats() {
    return service.getLocalExecutorStats();
  }

  @Override
  public void shutdown() {
    Span span = helper.buildSpan("shutdown", service);
    decorateAction(service::shutdown, span);
  }

  @Override
  public List<Runnable> shutdownNow() {
    Span span = helper.buildSpan("shutdownNow", service);
    return decorate(service::shutdownNow, span);
  }

  @Override
  public boolean isShutdown() {
    Span span = helper.buildSpan("isShutdown", service);
    return decorate(service::isShutdown, span);
  }

  @Override
  public boolean isTerminated() {
    Span span = helper.buildSpan("isTerminated", service);
    return decorate(service::isTerminated, span);
  }

  @Override
  public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
    Span span = helper.buildSpan("awaitTermination", service);
    span.setTag("timeout", timeout);
    span.setTag("timeUnit", nullable(unit));
    return decorateExceptionally(() -> service.awaitTermination(timeout, unit), span);
  }

  @Override
  public <T> Future<T> submit(Callable<T> task) {
    Span span = helper.buildSpan("submit", service);
    span.setTag("task", nullableClass(task));
    Map<String, String> spanContextMap = inject(span);
    TracingCallable<T> tracingCallable = new TracingCallable<>(task, traceWithActiveSpanOnly,
        spanContextMap);
    return decorate(() -> service.submit(tracingCallable), span);
  }

  @Override
  public <T> Future<T> submit(Runnable task, T result) {
    Span span = helper.buildSpan("submit", service);
    span.setTag("task", nullableClass(task));
    span.setTag("result", nullable(result));
    Map<String, String> spanContextMap = inject(span);
    TracingRunnable tracingRunnable = new TracingRunnable(task, traceWithActiveSpanOnly,
        spanContextMap);
    return decorate(() -> service.submit(tracingRunnable, result), span);
  }

  @Override
  public Future<?> submit(Runnable task) {
    Span span = helper.buildSpan("submit", service);
    span.setTag("task", nullableClass(task));
    Map<String, String> spanContextMap = inject(span);
    return decorate(() -> service.submit(
        new TracingRunnable(task, traceWithActiveSpanOnly, spanContextMap)), span);
  }

  @Override
  public <T> List<Future<T>> invokeAll(
      Collection<? extends Callable<T>> tasks) throws InterruptedException {
    Span span = helper.buildSpan("invokeAll", service);
    span.setTag("tasks", nullableClass(tasks));
    Map<String, String> spanContextMap = inject(span);
    List<TracingCallable<T>> tracingCallables = tasks.stream()
        .map(task -> new TracingCallable<>(task, traceWithActiveSpanOnly, spanContextMap))
        .collect(Collectors.toList());

    return decorateExceptionally(() -> service.invokeAll(tracingCallables), span);
  }

  @Override
  public <T> List<Future<T>> invokeAll(
      Collection<? extends Callable<T>> tasks, long timeout,
      TimeUnit unit) throws InterruptedException {
    Span span = helper.buildSpan("invokeAll", service);
    span.setTag("tasks", nullableClass(tasks));
    span.setTag("timeout", timeout);
    span.setTag("unit", nullableClass(unit));
    Map<String, String> spanContextMap = inject(span);
    List<TracingCallable<T>> tracingCallables = tasks.stream()
        .map(task -> new TracingCallable<>(task, traceWithActiveSpanOnly, spanContextMap))
        .collect(Collectors.toList());

    return decorateExceptionally(() -> service.invokeAll(tracingCallables, timeout, unit), span);
  }

  @Override
  public <T> T invokeAny(Collection<? extends Callable<T>> tasks)
      throws InterruptedException, ExecutionException {
    Span span = helper.buildSpan("invokeAny", service);
    span.setTag("tasks", nullableClass(tasks));
    Map<String, String> spanContextMap = inject(span);
    List<TracingCallable<T>> tracingCallables = tasks.stream()
        .map(task -> new TracingCallable<>(task, traceWithActiveSpanOnly, spanContextMap))
        .collect(Collectors.toList());

    return decorateExceptionally3(() -> service.invokeAny(tracingCallables), span);
  }

  @Override
  public <T> T invokeAny(Collection<? extends Callable<T>> tasks,
      long timeout, TimeUnit unit)
      throws InterruptedException, ExecutionException, TimeoutException {
    Span span = helper.buildSpan("invokeAny", service);
    span.setTag("tasks", nullableClass(tasks));
    span.setTag("timeout", timeout);
    span.setTag("unit", nullableClass(unit));
    Map<String, String> spanContextMap = inject(span);
    List<TracingCallable<T>> tracingCallables = tasks.stream()
        .map(task -> new TracingCallable<>(task, traceWithActiveSpanOnly, spanContextMap))
        .collect(Collectors.toList());

    return decorateExceptionally4(() -> service.invokeAny(tracingCallables, timeout, unit), span);
  }

  @Override
  public void execute(Runnable command) {
    Span span = helper.buildSpan("execute", service);
    span.setTag("command", nullableClass(command));
    Map<String, String> spanContextMap = inject(span);
    TracingRunnable tracingRunnable = new TracingRunnable(command, traceWithActiveSpanOnly,
        spanContextMap);
    decorateAction(() -> service.execute(tracingRunnable), span);
  }

  @Override
  public String getPartitionKey() {
    return service.getPartitionKey();
  }

  @Override
  public String getName() {
    return service.getName();
  }

  @Override
  public String getServiceName() {
    return service.getServiceName();
  }

  @Override
  public void destroy() {
    Span span = helper.buildSpan("destroy", service);
    decorateAction(service::destroy, span);
  }

}
