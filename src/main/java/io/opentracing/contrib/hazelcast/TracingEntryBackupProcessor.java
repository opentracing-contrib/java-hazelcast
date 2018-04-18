package io.opentracing.contrib.hazelcast;

import static io.opentracing.contrib.hazelcast.TracingHelper.decorateAction;
import static io.opentracing.contrib.hazelcast.TracingHelper.extract;
import static io.opentracing.contrib.hazelcast.TracingHelper.nullable;
import static io.opentracing.contrib.hazelcast.TracingHelper.nullableClass;

import com.hazelcast.map.EntryBackupProcessor;
import io.opentracing.Span;
import io.opentracing.SpanContext;
import java.util.Map;
import java.util.Map.Entry;

public class TracingEntryBackupProcessor<K, V> implements EntryBackupProcessor<K, V> {

  private final EntryBackupProcessor<K, V> entryBackupProcessor;
  private final boolean traceWithActiveSpanOnly;
  private final Map<String, String> spanContextMap;

  public TracingEntryBackupProcessor(
      EntryBackupProcessor<K, V> entryBackupProcessor, boolean traceWithActiveSpanOnly,
      Map<String, String> spanContextMap) {
    this.entryBackupProcessor = entryBackupProcessor;
    this.traceWithActiveSpanOnly = traceWithActiveSpanOnly;
    this.spanContextMap = spanContextMap;
  }

  @Override
  public void processBackup(Entry<K, V> entry) {
    SpanContext parent = extract(spanContextMap);
    Span span = TracingHelper.buildSpan("process", parent, traceWithActiveSpanOnly);
    span.setTag("entryBackupProcessor", nullableClass(entryBackupProcessor));
    span.setTag("entry", nullable(entry));
    decorateAction(() -> entryBackupProcessor.processBackup(entry), span);
  }
}
