package org.apache.hadoop.ryan;

import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.*;

/**
 * Created by ryan on 3/15/14.
 */
public class TimeLog extends BaseLog {

  public static long DEFAULT_MIN_NANOS = 50*1000*1000; // 50 ms
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

    public TimerTree() {
      this.nodes = new Stack<TimerNode<T>>();
      enterFunction(null, 0);
    }

    public void enterFunction(T data, long minTime) {
      invokedCount++;
      nodes.push(new TimerNode(data, minTime));
    }

    public void exitFunction(T data) {
      TimerNode node = nodes.pop();
      node.stop(data);
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
        val = new Long(0);
      } else {
        val = this.get(key);
      }
      this.put(key, val + value);
    }
  }

  private static class TimerNode<T> {

    private final MultiMap<T, TimerNode> children;
    private final SumMap<T> aggregates;
    private final long startTime;
    private long endTime;
    private long elapsedTime;
    private final long minTime;
    private T data;

    public TimerNode(T data, long minTime) {
      this.children = new MultiMap<T, TimerNode>();
      this.aggregates = new SumMap<T>();
      this.data = data;
      this.minTime = minTime;
      this.startTime = getTime();
    }

    public void stop() {
      stop(data);
    }

    public void stop(T data) {
      this.endTime = getTime();
      if (this.data != data && !this.data.equals(data)) {
        throw new IllegalStateException(this.data + " != " + data);
      }
      this.elapsedTime = endTime - startTime;
    }


    public long getTime() {
      return System.nanoTime();
    }

    public void addFinished(TimerNode<T> child) {
      if (child.elapsedTime < child.minTime) {
        for (Map.Entry<T, Long> agg : child.aggregates.entrySet()) {
          aggregates.add(agg.getKey(), agg.getValue()); // adopt child's aggregates
          child.elapsedTime -= agg.getValue(); // take away appropriate time from child
        }
        if (child.children.size() != 0) {
          throw new IllegalStateException("ryanlog: aggregate has non-agg child!");
        }
        aggregates.add(child.data, child.elapsedTime);
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
    end(name + " //" + resource);
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
      timers.get().exitFunction(name(name));
    }
    //info(name);
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
