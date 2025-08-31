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

import io.vertx.core.spi.transport.TransportConfig;

/**
 * Instances of this class are used to configure {@link io.netty.channel.kqueue.KQueueIoHandler} instances.
 * @author <a href="mailto:dreamlike.vert@gmail.com">dreamlikeOcean</a>
 */
public class KQueueTransportConfig implements TransportConfig {
  private static final int DEFAULT_MAX_EVENTS = 0;

  private int maxEvents = DEFAULT_MAX_EVENTS;


  /**
   * Set the maximum number of events to be processed per select.
   * @param maxEvents the maximum number of events to be processed per select
   * @return a reference to this, so the API can be used fluently
   */
  public KQueueTransportConfig setMaxEvents(int maxEvents) {
    if (maxEvents < 0) {
      throw new IllegalArgumentException("maxEvent must be >= 0");
    }
    this.maxEvents = maxEvents;
    return this;
  }

  /**
   * Get the maximum number of events to be processed per select.
   * @return the maximum number of events to be processed per select
   */
  public int getMaxEvents() {
    return maxEvents;
  }
}
