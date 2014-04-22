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
package org.apache.hadoop.fs;

import java.io.*;
import java.io.DataOutputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.ryan.OutputStreamWrapper;
import org.apache.hadoop.ryan.TimeLog;

/** Utility that wraps a {@link OutputStream} in a {@link DataOutputStream},
 * buffers output through a {@link BufferedOutputStream} and creates a checksum
 * file. */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class FSDataOutputStream extends DataOutputStream
    implements Syncable, CanSetDropBehind {
  private final OutputStream wrappedStream;

  private static class PositionCache extends FilterOutputStream {
    private FileSystem.Statistics statistics;
    long position;
    private final TimeLog timeLog = new TimeLog(PositionCache.class);

    public PositionCache(OutputStream out, 
                         FileSystem.Statistics stats,
                         long pos) throws IOException {
      super(out);
      timeLog.info("OutputStream class: " + out.getClass());
      timeLog.info("OutputStream" + getWrapped(out.getClass(), out, 20, new HashSet<Object>()));
      //TODO(ryan): consider using reflection to wrap out.out (which is a BufferedOutputStream)
      // then we can track all writes and have low overhead!
      statistics = stats;
      position = pos;
    }

    private List<Object> getWrapped(Class cls, Object base, int depth, Set<Object> avoid) {
      String field = "out";
      List<Object> ret = new ArrayList<Object>();
      if (depth < 0) {
        return ret;
      } else {
        ret.add(base);
        try {
          for (Field f : candidates(cls)) {
            boolean old = f.isAccessible();
            f.setAccessible(true);
            Object next = f.get(base);
            if (base instanceof  BufferedOutputStream && f.getName().equals("out") && !avoid.contains(base) &&
                !(next instanceof OutputStreamWrapper)) {
              f.set(base, new OutputStreamWrapper((OutputStream) next));
              timeLog.info("changing bufferedoutputstream out " + next + ": " + next.getClass());
              avoid.add(base);
            }
            f.setAccessible(old);
            ret.addAll(getWrapped(next.getClass(), next, depth - 1, avoid));
          }
        } catch (IllegalAccessException e) {
          e.printStackTrace();
        }
        return ret;
      }
    }

    private List<Field> candidates(Class cls) {
      List<Field> ret = new ArrayList<Field>();
      while (cls != Object.class) {
        for (Field f : cls.getDeclaredFields()) {
          if (OutputStream.class.isAssignableFrom(f.getType())) {
            ret.add(f);
          }
        }
        cls = cls.getSuperclass();
      }
      return ret;
    }

    public void write(int b) throws IOException {
      //timeLog.start("write(int b)", TimeLog.Resource.GENERAL_IO);
      try {
        out.write(b);
      } finally {
        //timeLog.end("write(int b)", TimeLog.Resource.GENERAL_IO);
      }
      position++;
      if (statistics != null) {
        statistics.incrementBytesWritten(1);
      }
    }
    
    public void write(byte b[], int off, int len) throws IOException {
      //timeLog.start("write(byte b[], int off, int len)", TimeLog.Resource.GENERAL_IO);
      try {
        out.write(b, off, len);
      } finally {
        //timeLog.end("write(byte b[], int off, int len)", TimeLog.Resource.GENERAL_IO);
      }
      position += len;                            // update position
      if (statistics != null) {
        statistics.incrementBytesWritten(len);
      }
    }
      
    public long getPos() throws IOException {
      return position;                            // return cached position
    }
    
    public void close() throws IOException {
      out.close();
    }
  }

  @Deprecated
  public FSDataOutputStream(OutputStream out) throws IOException {
    this(out, null);
  }

  public FSDataOutputStream(OutputStream out, FileSystem.Statistics stats)
    throws IOException {
    this(out, stats, 0);
  }

  private TimeLog timeLog = new TimeLog(FSDataOutputStream.class);

  public FSDataOutputStream(OutputStream out, FileSystem.Statistics stats,
                            long startPosition) throws IOException {
    super(new PositionCache(out, stats, startPosition));
    wrappedStream = out;
    timeLog.info("OutputStream class: " + out.getClass());
  }
  
  /**
   * Get the current position in the output stream.
   *
   * @return the current position in the output stream
   */
  public long getPos() throws IOException {
    return ((PositionCache)out).getPos();
  }

  /**
   * Close the underlying output stream.
   */
  public void close() throws IOException {
    out.close(); // This invokes PositionCache.close()
  }

  /**
   * Get a reference to the wrapped output stream. Used by unit tests.
   *
   * @return the underlying output stream
   */
  @InterfaceAudience.LimitedPrivate({"HDFS"})
  public OutputStream getWrappedStream() {
    return wrappedStream;
  }

  @Override  // Syncable
  @Deprecated
  public void sync() throws IOException {
    if (wrappedStream instanceof Syncable) {
      ((Syncable)wrappedStream).sync();
    }
  }
  
  @Override  // Syncable
  public void hflush() throws IOException {
    if (wrappedStream instanceof Syncable) {
      ((Syncable)wrappedStream).hflush();
    } else {
      wrappedStream.flush();
    }
  }
  
  @Override  // Syncable
  public void hsync() throws IOException {
    if (wrappedStream instanceof Syncable) {
      ((Syncable)wrappedStream).hsync();
    } else {
      wrappedStream.flush();
    }
  }

  @Override
  public void setDropBehind(Boolean dropBehind) throws IOException {
    try {
      ((CanSetDropBehind)wrappedStream).setDropBehind(dropBehind);
    } catch (ClassCastException e) {
      throw new UnsupportedOperationException("the wrapped stream does " +
          "not support setting the drop-behind caching setting.");
    }
  }
}
