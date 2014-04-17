package org.apache.hadoop.ryan;

import static java.lang.Math.pow;

/**
 * Created by ryan on 3/15/14.
 */
public class Timer {

  long start;
  private static double millisPerNano = pow(10, 6);

  public void start() {
    start = System.nanoTime();
  }

  public long nanoTime() {
    return System.nanoTime() - start;
  }

  public double milliTime() {
    return nanoTime() / millisPerNano;
  }

}

