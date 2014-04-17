package org.apache.hadoop.ryan;

import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by ryan on 3/15/14.
 */
public class BaseLog {

  private final Log logger;
  private long count;
  private final ObjectMapper mapper;
  private static final String MAGIC = "ryanlog"; // for scraping
  private final Timer timer;
  final String className;

  public BaseLog(Class clazz) {
    this.className = clazz.getName();
    logger = LogFactory.getLog(clazz);
    count = 0;
    mapper = new ObjectMapper();
    timer = new Timer();
  }

  private static long getId() {
    return Thread.currentThread().getId();
  }

  public void info(String string) {
    infoObj(string);
  }

  protected synchronized void infoObj(Object obj) {
    timer.start();
    String toWrite = serialize(getData(obj));
    double serializeElapsed = timer.milliTime();

    String toLog = "magic=" + MAGIC + ", renderTime=" + serializeElapsed + ", data=" + toWrite;
    logger.info("ryanlog: will log " + toLog.length() + " bytes");
    logger.info(toLog);
  }

  private String serialize(Map<Object, Object> data) {
    try {
      return mapper.writeValueAsString(data);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private Map<Object, Object> getData(Object obj) {
    Object additional = additional();
    Map<Object, Object> map = new HashMap();
    map.put("threadId", getId());
    map.put("count", count++);
    map.put("message", obj);
    map.put("additional", additional);
    map.put("infoElapsed", timer.milliTime());
    return map;
  }

  private String serializeFast(Map<Object, Object> data) {
    return data.toString();
  }

  protected Object additional() {
    return null;
  }

}
