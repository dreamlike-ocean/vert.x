/*
 * Copyright (c) 2011-2026 Contributors to the Eclipse Foundation
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License 2.0 which is available at
 * http://www.eclipse.org/legal/epl-2.0, or the Apache License, Version 2.0
 * which is available at https://www.apache.org/licenses/LICENSE-2.0.
 *
 * SPDX-License-Identifier: EPL-2.0 OR Apache-2.0
 */
package io.vertx.core.net.impl;

import io.netty.handler.stream.ChunkedNioFile;

import java.io.IOException;
import java.nio.channels.FileChannel;

public class VertxChunkedNioFile extends ChunkedNioFile {

  private final FileChannel channel;

  public VertxChunkedNioFile(FileChannel channel, long offset, long length) throws IOException {
    super(channel, offset, length, 8192);
    this.channel = channel;
  }

  public FileChannel channel() {
    return channel;
  }
}
