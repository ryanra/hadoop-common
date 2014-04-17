/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.mapred;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import org.apache.hadoop.mapred.Task.TaskReporter;
import org.apache.hadoop.ryan.TimeLog;

@InterfaceAudience.LimitedPrivate({"MapReduce"})
@InterfaceStability.Unstable
public interface MapOutputCollector<K, V> {
  public void init(Context context
                  ) throws IOException, ClassNotFoundException;
  public void collect(K key, V value, int partition
                     ) throws IOException, InterruptedException;
  public void close() throws IOException, InterruptedException;
    
  public void flush() throws IOException, InterruptedException, 
                             ClassNotFoundException;

  @InterfaceAudience.LimitedPrivate({"MapReduce"})
  @InterfaceStability.Unstable
  public static class Context {
    private final MapTask mapTask;
    private final JobConf jobConf;
    private final TaskReporter reporter;

    public Context(MapTask mapTask, JobConf jobConf, TaskReporter reporter) {
      this.mapTask = mapTask;
      this.jobConf = jobConf;
      this.reporter = reporter;
    }

    public MapTask getMapTask() {
      return mapTask;
    }

    public JobConf getJobConf() {
      return jobConf;
    }

    public TaskReporter getReporter() {
      return reporter;
    }
  }

  public class MapOutputCollectorWrapper<K, V> implements MapOutputCollector<K, V> {

    private final MapOutputCollector<K, V> wrapped;
    private final TimeLog timeLog = new TimeLog(MapOutputCollector.class);

    public MapOutputCollectorWrapper(MapOutputCollector<K, V> wrapped) {
      this.wrapped = wrapped;
    }

    @Override
    public void init(Context context) throws IOException, ClassNotFoundException {
      timeLog.start("init(Context context)");
      try {
        wrapped.init(context);
      } finally {
        timeLog.end("init(Context context)");
      }
    }

    @Override
    public void collect(K key, V value, int partition) throws IOException, InterruptedException {
      timeLog.start("collect(K key, V value, int partition)");
      try {
        wrapped.collect(key, value, partition);
      } finally {
        timeLog.end("collect(K key, V value, int partition)");
      }
    }

    @Override
    public void close() throws IOException, InterruptedException {
      timeLog.start("close()", TimeLog.Resource.DISK);
      try {
        wrapped.close();
      } finally {
        timeLog.end("close()", TimeLog.Resource.DISK);
      }
    }

    @Override
    public void flush() throws IOException, InterruptedException, ClassNotFoundException {
      timeLog.start("flush()", TimeLog.Resource.DISK);
      try {
        wrapped.flush();
      } finally {
        timeLog.end("flush()", TimeLog.Resource.DISK);
      }
    }
  }

}
