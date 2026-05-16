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
package io.vertx.core.impl.transports.iouring;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http2.DefaultHttp2DataFrame;
import io.netty.handler.codec.http2.Http2DataFrame;
import io.netty.handler.codec.http2.Http2FrameStream;
import io.netty.handler.stream.ChunkedInput;
import io.vertx.core.http.impl.http2.multiplex.VertxHttp2DataChunkedInput;

class IoUringHttp2DataChunkedInput implements ChunkedInput<Http2DataFrame> {

  private final IoUringChunkedFile input;
  private final Http2FrameStream stream;
  private boolean endStreamSent;

  IoUringHttp2DataChunkedInput(VertxHttp2DataChunkedInput input, IoUringChunkedWriteHandler handler) throws Exception {
    this.stream = input.stream();
    this.input = new IoUringChunkedFile(input.input(), handler);
  }

  @Override
  public boolean isEndOfInput() throws Exception {
    if (input.isEndOfInput()) {
      // Only end of input after last HTTP chunk has been sent
      return endStreamSent;
    }
    return false;
  }

  @Override
  public void close() throws Exception {
    input.close();
  }

  @Override
  public Http2DataFrame readChunk(ChannelHandlerContext ctx) throws Exception {
    return readChunk(ctx.alloc());
  }

  @Override
  public Http2DataFrame readChunk(ByteBufAllocator allocator) throws Exception {
    if (endStreamSent) {
      return null;
    }
    if (input.isEndOfInput()) {
      endStreamSent = true;
      return new DefaultHttp2DataFrame(true).stream(stream);
    }
    ByteBuf chunk = input.readChunk(allocator);
    if (chunk == null) {
      return null;
    }
    Http2DataFrame frame = new DefaultHttp2DataFrame(chunk, input.isEndOfInput()).stream(stream);
    if (frame.isEndStream()) {
      endStreamSent = true;
    }
    return frame;
  }

  @Override
  public long length() {
    return input.length();
  }

  @Override
  public long progress() {
    return input.progress();
  }
}
