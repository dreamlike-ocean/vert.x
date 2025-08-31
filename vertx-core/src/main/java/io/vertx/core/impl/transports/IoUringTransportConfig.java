/*
 * Copyright (c) 2011-2025 Contributors to the Eclipse Foundation
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License 2.0 which is available at
 * http://www.eclipse.org/legal/epl-2.0, or the Apache License, Version 2.0
 * which is available at https://www.apache.org/licenses/LICENSE-2.0.
 *
 * SPDX-License-Identifier: EPL-2.0 OR Apache-2.0
 */
package io.vertx.core.impl.transports;

import io.netty.channel.uring.IoUring;
import io.netty.channel.uring.IoUringBufferRingConfig;
import io.netty.channel.uring.IoUringFixedBufferRingAllocator;
import io.netty.channel.uring.IoUringIoHandlerConfig;
import io.vertx.core.spi.transport.TransportConfig;

/**
 * Instances of this class are used to configure {@link io.netty.channel.uring.IoUringIoHandler} instances.
 * And you can find every option`s meaning in {@link io.netty.channel.uring.IoUringIoHandlerConfig}.
 * @author <a href="mailto:dreamlike.vert@gmail.com">dreamlikeOcean</a>
 */
public final class IoUringTransportConfig implements TransportConfig {

  private static final short BUFFER_GROUP_ID = 1;
  private static final int DEFAULT_RING_SIZE;
  private static final int DEFAULT_CQ_SIZE;
  private static final int DEFAULT_MAX_BOUNDED_WORKER;
  private static final int DEFAULT_MAX_UNBOUNDED_WORKER;

  private int ringSize = DEFAULT_RING_SIZE;
  private int cqSize = DEFAULT_CQ_SIZE;
  private int maxBoundedWorker = DEFAULT_MAX_BOUNDED_WORKER;
  private int maxUnboundedWorker = DEFAULT_MAX_UNBOUNDED_WORKER;
  private IoUringBufferRingConfig bufferRingConfig;

  /**
   * Sets the size of the submission queue for the io_uring instance.
   *
   * @param ringSize the size of the submission queue
   * @return a reference to this, so the API can be used fluently
   */
  public IoUringTransportConfig setRingSize(int ringSize) {
    if (ringSize < 1) {
      throw new IllegalArgumentException("ringSize must be > 0");
    }
    this.ringSize = ringSize;
    return this;
  }

  /**
   * Returns the size of the submission queue for the io_uring instance.
   * @return the size of the submission queue
   */
  public int getRingSize() {
    return ringSize;
  }

  /**
   * Set the size of the completion queue for the io_uring instance.
   * @param cqSize the size of the completion queue
   * @return a reference to this, so the API can be used fluently
   */
  public IoUringTransportConfig setCqSize(int cqSize) {
    if (cqSize < 1) {
      throw new IllegalArgumentException("cqSize must be > 0");
    }
    boolean isPowerOfTwo = Integer.bitCount(cqSize) == 1;
    if (!isPowerOfTwo) {
      throw new IllegalArgumentException("cqSize: " + cqSize + " (expected: power of 2)");
    }
    if (cqSize < ringSize) {
      throw new IllegalArgumentException("cqSize must be greater than or equal to ringSize");
    }
    return this;
  }

  /**
   * Returns the size of the completion queue.
   * @return the size of the completion queue
   */
  public int getCqSize() {
    return cqSize;
  }

  /**
   * Sets the maximum number of bounded worker threads.
   * @param maxBoundedWorker the maximum number of bounded worker threads
   * @return a reference to this, so the API can be used fluently
   */
  public IoUringTransportConfig setMaxBoundedWorker(int maxBoundedWorker) {
    if (maxBoundedWorker < 0) {
      throw new IllegalArgumentException("maxBoundedWorker must be >= 0");
    }
    this.maxBoundedWorker = maxBoundedWorker;
    return this;
  }

  /**
   * Get the maximum number of bounded worker threads
   * @return the maximum number of bounded worker threads
   */
  public int getMaxBoundedWorker() {
    return maxBoundedWorker;
  }

  /**
   * Set the maximum number of unbounded worker threads
   * @param maxUnboundedWorker the maximum number of unbounded worker threads
   * @return a reference to this, so the API can be used fluently
   */
  public IoUringTransportConfig setMaxUnboundedWorker(int maxUnboundedWorker) {
    if (maxUnboundedWorker < 0) {
      throw new IllegalArgumentException("maxUnboundedWorker must be >= 0");
    }
    this.maxUnboundedWorker = maxUnboundedWorker;
    return this;
  }

  /**
   * Get the maximum number of unbounded worker threads
   * @return the maximum number of unbounded worker threads
   */
  public int getMaxUnboundedWorker() {
    return maxUnboundedWorker;
  }

  /**
   *  Add a buffer ring configuration to the list of buffer ring configurations.
   * @param bufferRingSize the size of the buffer ring
   * @param bufferSize the size of single buffer
   * @return a reference to this, so the API can be used fluently
   */
  public IoUringTransportConfig setBufferRing(short bufferRingSize, int bufferSize) {
    if (!IoUring.isRegisterBufferRingSupported()) {
      throw new UnsupportedOperationException("current os dont support io_uring_register_buffer_ring");
    }

    bufferRingSize = checkBufferRingSize(bufferRingSize);
    this.bufferRingConfig = IoUringBufferRingConfig.builder()
      .bufferGroupId(BUFFER_GROUP_ID)
      .bufferRingSize(bufferRingSize)
      .allocator(new IoUringFixedBufferRingAllocator(bufferSize))
      .build();
    return this;
  }

  IoUringBufferRingConfig getBufferRingConfig() {
    return bufferRingConfig;
  }

  private static short checkBufferRingSize(short bufferRingSize) {
    if (bufferRingSize < 1) {
      throw new IllegalArgumentException("bufferRingSize: " + bufferRingSize + " (expected: > 0)");
    }

    boolean isPowerOfTwo = (bufferRingSize & (bufferRingSize - 1)) == 0;
    if (!isPowerOfTwo) {
      throw new IllegalArgumentException("bufferRingSize: " + bufferRingSize + " (expected: power of 2)");
    }
    return bufferRingSize;
  }

  static {
    IoUringIoHandlerConfig ioUringIoHandlerConfig = new IoUringIoHandlerConfig();
    DEFAULT_RING_SIZE = ioUringIoHandlerConfig.getRingSize();
    DEFAULT_CQ_SIZE = ioUringIoHandlerConfig.getCqSize();
    DEFAULT_MAX_BOUNDED_WORKER = ioUringIoHandlerConfig.getMaxBoundedWorker();
    DEFAULT_MAX_UNBOUNDED_WORKER = ioUringIoHandlerConfig.getMaxUnboundedWorker();
  }
}
