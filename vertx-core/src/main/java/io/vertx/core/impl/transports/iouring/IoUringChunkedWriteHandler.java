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
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.channel.EventLoop;
import io.netty.channel.IoEvent;
import io.netty.channel.IoEventLoop;
import io.netty.channel.IoRegistration;
import io.netty.channel.unix.Errors;
import io.netty.channel.uring.IoUringIoEvent;
import io.netty.channel.uring.IoUringIoHandle;
import io.netty.channel.uring.IoUringIoOps;
import io.netty.handler.stream.ChunkedWriteHandler;
import io.vertx.core.http.impl.http2.multiplex.VertxHttp2DataChunkedInput;
import io.vertx.core.internal.logging.Logger;
import io.vertx.core.internal.logging.LoggerFactory;
import io.vertx.core.net.impl.VertxChunkedNioFile;

import java.io.EOFException;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.nio.channels.FileChannel;

/**
 * {@link ChunkedWriteHandler} variant that reads {@link IoUringChunkedFile} chunks with io_uring.
 */
public class IoUringChunkedWriteHandler extends ChunkedWriteHandler {

  private static final int ERRNO_ECANCELED_NEGATIVE = -125;
  private static final byte IORING_OP_ASYNC_CANCEL = 14;
  private static final byte IORING_OP_READ = 22;
  private static final Logger log = LoggerFactory.getLogger(IoUringChunkedWriteHandler.class);
  private static final MethodHandle FILE_CHANNEL_FD = fileChannelFdMethodHandle();
  private final IoUringChunkedReadIoHandle ioUringChunkedReadIoHandle = new IoUringChunkedReadIoHandle();
  private IoRegistration registration;

  public static boolean isFileChannelFdAvailable() {
    return FILE_CHANNEL_FD != null;
  }

  static int fileChannelFd(FileChannel fileChannel) throws Exception {
    try {
      return (int) FILE_CHANNEL_FD.invokeExact(fileChannel);
    } catch (Exception e) {
      throw e;
    } catch (Throwable t) {
      throw new IllegalStateException(t);
    }
  }

  private static MethodHandle fileChannelFdMethodHandle() {
    try {
      Class<?> fileChannelImpl = Class.forName("sun.nio.ch.FileChannelImpl");
      MethodHandles.Lookup lookup = MethodHandles.lookup();
      MethodHandle fileDescriptor = MethodHandles.privateLookupIn(fileChannelImpl, lookup)
        .findGetter(fileChannelImpl, "fd", java.io.FileDescriptor.class);
      MethodHandle fd = MethodHandles.privateLookupIn(java.io.FileDescriptor.class, lookup)
        .findGetter(java.io.FileDescriptor.class, "fd", int.class);
      return MethodHandles.filterReturnValue(fileDescriptor, fd)
        .asType(MethodType.methodType(int.class, FileChannel.class));
    } catch (ReflectiveOperationException | SecurityException e) {
      log.info("io_uring chunked file reads are not available because FileChannel fd access is unavailable; " +
        "falling back to the default ChunkedWriteHandler. Enable this feature with JVM options: " +
        "--add-opens=java.base/sun.nio.ch=" + moduleName() + " --add-opens=java.base/java.io=" + moduleName(), e);
      return null;
    }
  }

  private static String moduleName() {
    Module module = IoUringChunkedWriteHandler.class.getModule();
    return module.isNamed() ? module.getName() : "ALL-UNNAMED";
  }

  @Override
  public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
    EventLoop eventLoop = ctx.channel().eventLoop();
    if (!(eventLoop instanceof IoEventLoop)) {
      throw new IllegalArgumentException("IoUringChunkedWriteHandler can only be used with an IoEventLoop");
    }
    IoEventLoop ioEventLoop = (IoEventLoop) eventLoop;
    if (!ioEventLoop.isCompatible(IoUringChunkedReadIoHandle.class)) {
      throw new IllegalArgumentException("IoUringChunkedWriteHandler can only be used with an io_uring IoEventLoop");
    }
    super.handlerAdded(ctx);
    registration = ioEventLoop.register(ioUringChunkedReadIoHandle).getNow();
  }

  @Override
  public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
    if (msg instanceof VertxHttp2DataChunkedInput) {
      msg = new IoUringHttp2DataChunkedInput((VertxHttp2DataChunkedInput) msg, this);
    } else if (msg instanceof VertxChunkedNioFile) {
      msg = new IoUringChunkedFile((VertxChunkedNioFile) msg, this);
    }
    super.write(ctx, msg, promise);
  }

  void requestAsyncRead(IoUringChunkedFile input, int fd, long offset, ByteBuf readBuffer) {
    ioUringChunkedReadIoHandle.read(input, fd, offset, readBuffer);
  }

  boolean cancel(IoUringChunkedFile input) {
    return ioUringChunkedReadIoHandle.cancel(input);
  }

  @Override
  public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
    ioUringChunkedReadIoHandle.removed();
    super.handlerRemoved(ctx);
  }

  final class IoUringChunkedReadIoHandle implements IoUringIoHandle {

    private IoUringChunkedFile currentInput;
    private long readId;

    void read(IoUringChunkedFile input, int fd, long offset, ByteBuf readBuffer) {
      currentInput = input;
      long memoryAddress = readBuffer.memoryAddress() + readBuffer.writerIndex();
      readId = registration.submit(new IoUringIoOps(IORING_OP_READ, (byte) 0, (short) 0, fd, offset,
        memoryAddress, readBuffer.writableBytes(), 0, (short) 0, (short) 0, (short) 0, 0, 0));
    }

    boolean cancel(IoUringChunkedFile input) {
      if (currentInput != input) {
        return false;
      }
      cancelRead();
      return true;
    }

    @Override
    public void close() {
      removed();
    }

    void removed() {
      cancelRead();
      IoRegistration r = registration;
      if (r != null) {
        registration = null;
        r.cancel();
      }
    }

    @Override
    public void handle(IoRegistration registration, IoEvent event) {
      IoUringIoEvent uringEvent = (IoUringIoEvent) event;
      if (uringEvent.opcode() == IORING_OP_ASYNC_CANCEL) {
        // The original read CQE owns the chunk state transition.
        return;
      }
      completeRead(uringEvent.res());
    }

    private void cancelRead() {
      if (readId != 0 && registration != null) {
        registration.submit(new IoUringIoOps(IORING_OP_ASYNC_CANCEL, (byte) 0, (short) 0, -1, 0, readId, 0, 0,
          0, (short) 0, (short) 0, 0, 0));
        readId = 0;
      }
    }

    private void completeRead(int res) {
      IoUringChunkedFile input = currentInput;
      currentInput = null;
      // readId is cleared when a cancel has already been submitted.
      boolean cancelled = readId == 0;
      readId = 0;
      // A cancelled read can still complete successfully if it wins the race with ASYNC_CANCEL.
      if (res == ERRNO_ECANCELED_NEGATIVE || cancelled) {
        input.handleReadCancelled();
        return;
      }
      if (res == 0) {
        // A zero-byte io_uring file read means EOF, not a successful empty chunk.
        input.handleReadFailure(new EOFException("Unexpected EOF while reading file chunk (progress=" +
          input.progress() + ", length=" + input.length() + ')'));
      } else if (res > 0) {
        input.handleReadSuccess(res);
      } else {
        input.handleReadFailure(Errors.newIOException("io_uring read", res));
      }
    }
  }
}
