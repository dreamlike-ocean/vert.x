/*
 * Copyright (c) 2011-2019 Contributors to the Eclipse Foundation
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License 2.0 which is available at
 * http://www.eclipse.org/legal/epl-2.0, or the Apache License, Version 2.0
 * which is available at https://www.apache.org/licenses/LICENSE-2.0.
 *
 * SPDX-License-Identifier: EPL-2.0 OR Apache-2.0
 */
package io.vertx.core.impl.transports.iouring;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.stream.ChunkedInput;
import io.netty.util.ReferenceCountUtil;
import io.vertx.core.net.impl.VertxChunkedNioFile;

class IoUringChunkedFile implements ChunkedInput<ByteBuf> {

  public static final int DEFAULT_CHUNK_SIZE = 8192;

  private final VertxChunkedNioFile input;
  private final int fd;
  private final long endOffset;
  private long offset;
  private long progress;
  private final IoUringChunkedWriteHandler handler;
  private ByteBuf readBuffer;
  private boolean readPending;
  private Exception cause;

  IoUringChunkedFile(VertxChunkedNioFile input, IoUringChunkedWriteHandler handler) throws Exception {
    this.input = input;
    this.fd = IoUringChunkedWriteHandler.fileChannelFd(input.channel());
    this.offset = input.currentOffset();
    this.endOffset = input.endOffset();
    this.handler = handler;
  }

  void handleReadSuccess(int readBytes) {
    readPending = false;
    offset += readBytes;
    progress += readBytes;
    readBuffer.writerIndex(readBuffer.writerIndex() + readBytes);
    handler.resumeTransfer();
  }

  void handleReadFailure(Exception cause) {
    this.cause = cause;
    readPending = false;
    ReferenceCountUtil.release(readBuffer);
    readBuffer = null;
    handler.resumeTransfer();
  }

  void handleReadCancelled() {
    readPending = false;
    ReferenceCountUtil.release(readBuffer);
    readBuffer = null;
    closeInput();
  }

  @Override
  public boolean isEndOfInput() {
    return !readPending && readBuffer == null && offset >= endOffset;
  }

  @Override
  public void close() throws Exception {
    if (readBuffer != null) {
      if (readPending && handler.cancel(this)) {
        return;
      }
      ReferenceCountUtil.release(readBuffer);
      readBuffer = null;
      readPending = false;
    }
    input.close();
  }

  @Override
  public ByteBuf readChunk(ChannelHandlerContext ctx) throws Exception {
    return readChunk(ctx.alloc());
  }

  @Override
  public ByteBuf readChunk(ByteBufAllocator allocator) throws Exception {
    // Suspend until the io_uring read completes; traffic shaping may trigger extra flushes.
    if (readPending) {
      return null;
    }
    if (cause != null) {
      throw cause;
    }
    ByteBuf readBuffer = this.readBuffer;
    if (readBuffer != null) {
      this.readBuffer = null;
      return readBuffer;
    }
    if (offset >= endOffset) {
      return null;
    }
    int chunkSize = (int) Math.min(DEFAULT_CHUNK_SIZE, endOffset - offset);
    readBuffer = allocator.directBuffer(chunkSize);
    this.readBuffer = readBuffer;
    readPending = true;
    handler.requestAsyncRead(this, fd, offset, readBuffer);
    return null;
  }

  @Override
  public long length() {
    return endOffset - offset + progress;
  }

  @Override
  public long progress() {
    return progress;
  }

  private void closeInput() {
    try {
      input.close();
    } catch (Exception ignore) {
    }
  }
}
