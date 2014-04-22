package org.apache.hadoop.ryan;

import java.io.IOException;
import java.io.OutputStream;

/**
 * Created by ryan on 4/17/14.
 */
public class OutputStreamWrapper extends OutputStream {

  private final OutputStream wrapped;
  private final TimeLog timeLog;
  private final String wrappedName;

  public OutputStreamWrapper(OutputStream wrapped) {
    this.wrapped = wrapped;
    this.wrappedName = wrapped.getClass().toString();
    this.timeLog = new TimeLog(OutputStreamWrapper.class);
  }

  private void logStart(String string) {
    timeLog.start(wrappedName + "." + string);
  }

  private void logEnd(String string) {
    timeLog.end(wrappedName + "." + string);
  }

  @Override
  public void write(int i) throws IOException {
    logStart("write(int i)");
    try {
      wrapped.write(i);
    } finally {
      logEnd("write(int i)");
    }
  }

  @Override
  public void close() throws IOException {
    logStart("close()");
    try {
      wrapped.close();
    } finally {
      logEnd("close()");
    }
  }

  @Override
  public void flush() throws IOException {
    logStart("flush()");
    try {
      wrapped.flush();
    } finally {
      logEnd("flush()");
    }
  }

  @Override
  public void write(byte[] b) throws IOException {
    logStart("write(byte[] b)");
    try {
      wrapped.write(b);
    } finally {
      logEnd("write(byte[] b)");
    }
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    logStart("write(byte[] b, int off, int len)");
    try {
      wrapped.write(b, off, len);
    } finally {
      logEnd("write(byte[] b, int off, int len)");
    }
  }

}
