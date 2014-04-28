package org.apache.hadoop.ryan;

import java.io.IOException;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.util.*;

/**
 * Created by ryan on 3/15/14.
 */
public class TimeLog extends BaseLog {

  private static int NANOS_PER_MILLI = 1000 * 1000;
  public static long DEFAULT_MIN_NANOS = 50*NANOS_PER_MILLI; // 50 ms
  private static final NestedTimers timers = new NestedTimers();
  private static final ThreadLocal<Boolean> timingEnabled = new ThreadLocal<Boolean>();

  private static class NestedTimers extends ThreadLocal<TimerTree<String>> {

    @Override
    public TimerTree initialValue() {
      System.out.println("ryanlog: a new timer tree !");
      return new TimerTree();
    }

  }

  private static class TimerTree<T> {

    private final Stack<TimerNode<T>> nodes;
    private long invokedCount = 0;
    private final long gcStart = getGCTime();

    public TimerTree() {
      this.nodes = new Stack<TimerNode<T>>();
      enterFunction(null, 0);
    }

    private static long getGCTime() {
      long sum = 0;
      for (GarbageCollectorMXBean bean : ManagementFactory.getGarbageCollectorMXBeans()) {
        sum += bean.getCollectionTime();
      }
      return sum * NANOS_PER_MILLI;
    }

    public void enterFunction(T data, long minTime) {
      invokedCount++;
      nodes.push(new TimerNode(data, minTime));
    }

    public void exitFunction(T data, long info) {
      TimerNode node = nodes.pop();
      node.stop(data, info);
      nodes.peek().addFinished(node);
    }

    public Map extractTimes() {
      if (nodes.size() != 1) {
        throw new IllegalStateException();
      }
      TimerNode node = nodes.pop();
      node.stop();
      Map mapified = node.mapify();
      Map<String, Object> topLevel = new HashMap<String, Object>();
      topLevel.put("timerRoot", mapified);
      topLevel.put("invokedCount", invokedCount);
      topLevel.put("gcTime", getGCTime() - gcStart);
      return topLevel;
    }
  }

  private static class MultiMap<K, V> extends HashMap<K, List<V>> {

    public void add(K key, V value) {
      List<V> vals;
      if (!this.containsKey(key)) {
        vals = new ArrayList<V>();
      } else {
        vals = this.get(key);
      }
      vals.add(value);
      this.put(key, vals);
    }
  }

  private static class SumMap<K> extends HashMap<K, Long> {

    public void add(K key, Long value) {
      Long val;
      if (!this.containsKey(key)) {
        val = 0L;
      } else {
        val = this.get(key);
      }
      this.put(key, val + value);
    }
  }

  private static class Pair<A, B> {

    public final A first;
    public final B second;

    public Pair(A first, B second) {
      this.first = first;
      this.second = second;
    }

  }

  private static class SumPairMap<K> extends HashMap<K, Pair<Long, Long>> {

    public void add(K key, Pair<Long, Long> value) {
      Pair<Long, Long> current;
      if (!this.containsKey(key)) {
        current = new Pair<Long, Long>(0L, 0L);
      } else {
        current = this.get(key);
      }
      this.put(key, new Pair(current.first + value.first, current.second + value.second));
    }
  }

  private static class TimerNode<T> {

    private final MultiMap<T, TimerNode> children;
    private final SumMap<T> aggregates;
    private final SumMap<T> infos;
    private final long startTime;
    private long endTime;
    private long elapsedTime;
    private final long minTime;
    private T data;
    private long info;

    public TimerNode(T data, long minTime) {
      this.children = new MultiMap<T, TimerNode>();
      this.aggregates = new SumMap<T>();
      this.infos = new SumMap<T>();
      this.data = data;
      this.minTime = minTime;
      this.startTime = getTime();
      this.info = -10;
    }

    public void stop() {
      stop(data, -5);
    }

    public void stop(T data, long info) {
      this.endTime = getTime();
      if (this.data != data && !this.data.equals(data)) {
        throw new IllegalStateException(this.data + " != " + data);
      }
      this.elapsedTime = endTime - startTime;
      this.info = info;
    }


    public long getTime() {
      return System.nanoTime();
    }

    public void addFinished(TimerNode<T> child) {
      if (child.elapsedTime < child.minTime) {
        for (Map.Entry<T, Long> agg : child.aggregates.entrySet()) {
          aggregates.add(agg.getKey(), agg.getValue()); // adopt child's aggregates
          child.elapsedTime -= agg.getValue(); // take away appropriate time from child
          long grandChildInfo = child.infos.get(agg.getKey());
          infos.add(agg.getKey(), grandChildInfo);
        }
        if (child.children.size() != 0) {
          throw new IllegalStateException("ryanlog: aggregate has non-agg child!");
        }
        aggregates.add(child.data, child.elapsedTime);
        infos.add(child.data, child.info);
      } else {
        children.add(child.data, child);
      }
    }


    List<Map> mapifyChildren() {
      List<Map> l = new ArrayList<Map>();
      for (List<TimerNode> nodes : children.values()) {
        for (TimerNode n : nodes) {
          l.add(n.mapify());
        }
      }
      return l;
    }

    public Map mapify() {
      Map stuff = new HashMap();
      stuff.put("data", data);
      stuff.put("startTime", startTime);
      stuff.put("endTime", endTime);
      stuff.put("elapsedTime", elapsedTime);
      stuff.put("aggregates", aggregates);
      stuff.put("combinedInfo", infos);
      stuff.put("info", info);
      stuff.put("children", mapifyChildren());
      return stuff;
    }

  }

  public TimeLog(Class clazz) {
    super(clazz);
  }

  public void start(String name, Resource resource) {
    start(name + " //" + resource);
  }

  public void end(String name, Resource resource) {
    end(name, resource, 1L);
  }

  public void start(String name) {
    start(name, DEFAULT_MIN_NANOS);
  }

  public void start(String name, long minTime) {
    if (isTimingEnabled()) {
      timers.get().enterFunction(name(name), minTime);
    }
  }

  public void end(String name) {
    if (isTimingEnabled()) {
      timers.get().exitFunction(name(name), 1L);
    }
  }

  public void end(String name, long info) {
    if (isTimingEnabled()) {
      timers.get().exitFunction(name(name), info);
    }
  }

  public void end(String name, Resource resource, long info) {
    end(name + " //" + resource, info);
  }

  private String name(String name) {
    return this.className + "." + name;
  }

  private boolean isTimingEnabled() {
    Boolean value = timingEnabled.get();
    return value != null && value == true; // rule out null
  }

  public void enableLogging() {
    timingEnabled.set(true);
  }

  public void logTimingData() {
    infoObj(timers.get().extractTimes());
    timers.remove();
  }

  public enum Resource {
    DISK, NETWORK, GENERAL_IO, CPU, SHUFFLE, SPILL // TODO: get rid of general io
  }

}
