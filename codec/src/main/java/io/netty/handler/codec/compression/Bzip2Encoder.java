/*
 * Copyright 2014 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package io.netty.handler.codec.compression;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.MessageToByteEncoder;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.Promise;
import io.netty.util.concurrent.PromiseNotifier;

import java.util.concurrent.TimeUnit;

import static io.netty.handler.codec.compression.Bzip2Constants.BASE_BLOCK_SIZE;
import static io.netty.handler.codec.compression.Bzip2Constants.END_OF_STREAM_MAGIC_1;
import static io.netty.handler.codec.compression.Bzip2Constants.END_OF_STREAM_MAGIC_2;
import static io.netty.handler.codec.compression.Bzip2Constants.MAGIC_NUMBER;
import static io.netty.handler.codec.compression.Bzip2Constants.MAX_BLOCK_SIZE;
import static io.netty.handler.codec.compression.Bzip2Constants.MIN_BLOCK_SIZE;

/**
 * Compresses a {@link ByteBuf} using the Bzip2 algorithm.
 *
 * See <a href="https://en.wikipedia.org/wiki/Bzip2">Bzip2</a>.
 */
public class Bzip2Encoder extends MessageToByteEncoder<ByteBuf> {
    /**
     * Current state of stream.
     */
    private enum State {
        INIT,
        INIT_BLOCK,
        WRITE_DATA,
        CLOSE_BLOCK
    }

    private State currentState = State.INIT;

    /**
     * A writer that provides bit-level writes.
     */
    private final Bzip2BitWriter writer = new Bzip2BitWriter();

    /**
     * The declared maximum block size of the stream (before final run-length decoding).
     */
    private final int streamBlockSize;

    /**
     * The merged CRC of all blocks compressed so far.
     */
    private int streamCRC;

    /**
     * The compressor for the current block.
     */
    private Bzip2BlockCompressor blockCompressor;

    /**
     * (@code true} if the compressed stream has been finished, otherwise {@code false}.
     */
    private volatile boolean finished;

    /**
     * Used to interact with its {@link ChannelPipeline} and other handlers.
     */
    private volatile ChannelHandlerContext ctx;

    /**
     * Creates a new bzip2 encoder with the maximum (900,000 byte) block size.
     */
    public Bzip2Encoder() {
        this(MAX_BLOCK_SIZE);
    }

    /**
     * Creates a new bzip2 encoder with the specified {@code blockSizeMultiplier}.
     * @param blockSizeMultiplier
     *        The Bzip2 block size as a multiple of 100,000 bytes (minimum {@code 1}, maximum {@code 9}).
     *        Larger block sizes require more memory for both compression and decompression,
     *        but give better compression ratios. {@code 9} will usually be the best value to use.
     */
    public Bzip2Encoder(final int blockSizeMultiplier) {
        if (blockSizeMultiplier < MIN_BLOCK_SIZE || blockSizeMultiplier > MAX_BLOCK_SIZE) {
            throw new IllegalArgumentException(
                    "blockSizeMultiplier: " + blockSizeMultiplier + " (expected: 1-9)");
        }
        streamBlockSize = blockSizeMultiplier * BASE_BLOCK_SIZE;
    }

    @Override
    protected void encode(ChannelHandlerContext ctx, ByteBuf in, ByteBuf out) throws Exception {
        if (finished) {
            out.writeBytes(in);
            return;
        }

        for (;;) {
            switch (currentState) {
                case INIT:
                    out.ensureWritable(4);
                    out.writeMedium(MAGIC_NUMBER);
                    out.writeByte('0' + streamBlockSize / BASE_BLOCK_SIZE);
                    currentState = State.INIT_BLOCK;
                    // fall through
                case INIT_BLOCK:
                    blockCompressor = new Bzip2BlockCompressor(writer, streamBlockSize);
                    currentState = State.WRITE_DATA;
                    // fall through
                case WRITE_DATA:
                    if (!in.isReadable()) {
                        return;
                    }
                    Bzip2BlockCompressor blockCompressor = this.blockCompressor;
                    final int length = Math.min(in.readableBytes(), blockCompressor.availableSize());
                    final int bytesWritten = blockCompressor.write(in, in.readerIndex(), length);
                    in.skipBytes(bytesWritten);
                    if (!blockCompressor.isFull()) {
                        if (in.isReadable()) {
                            break;
                        } else {
                            return;
                        }
                    }
                    currentState = State.CLOSE_BLOCK;
                    // fall through
                case CLOSE_BLOCK:
                    closeBlock(out);
                    currentState = State.INIT_BLOCK;
                    break;
                default:
                    throw new IllegalStateException();
            }
        }
    }

    /**
     * Close current block and update {@link #streamCRC}.
     */
    private void closeBlock(ByteBuf out) {
        final Bzip2BlockCompressor blockCompressor = this.blockCompressor;
        if (!blockCompressor.isEmpty()) {
            blockCompressor.close(out);
            final int blockCRC = blockCompressor.crc();
            streamCRC = (streamCRC << 1 | streamCRC >>> 31) ^ blockCRC;
        }
    }

    /**
     * Returns {@code true} if and only if the end of the compressed stream has been reached.
     */
    public boolean isClosed() {
        return finished;
    }

    /**
     * Close this {@link Bzip2Encoder} and so finish the encoding.
     *
     * The returned {@link Future} will be notified once the operation completes.
     */
    public Future<Void> close() {
        ChannelHandlerContext ctx = ctx();
        EventExecutor executor = ctx.executor();
        if (executor.inEventLoop()) {
            return finishEncode(ctx);
        } else {
            Promise<Void> promise = ctx.newPromise();
            executor.execute(() -> {
                Future<Void> f = finishEncode(ctx());
                PromiseNotifier.cascade(f, promise);
            });
            return promise;
        }
    }

    @Override
    public Future<Void> close(final ChannelHandlerContext ctx) {
        Future<Void> f = finishEncode(ctx);
        if (f.isDone()) {
            return ctx.close();
        }
        Promise<Void> promise = ctx.newPromise();
        f.addListener(f1 -> ctx.close().addListener(new PromiseNotifier<>(false, promise)));
        // Ensure the channel is closed even if the write operation completes in time.
        ctx.executor().schedule(() -> {
            ctx.close().addListener(new PromiseNotifier<>(false, promise));
        }, 10, TimeUnit.SECONDS); // FIXME: Magic number
        return promise;
    }

    private Future<Void> finishEncode(final ChannelHandlerContext ctx) {
        if (finished) {
            return ctx.newSucceededFuture();
        }
        finished = true;

        final ByteBuf footer = ctx.alloc().buffer();
        closeBlock(footer);

        final int streamCRC = this.streamCRC;
        final Bzip2BitWriter writer = this.writer;
        try {
            writer.writeBits(footer, 24, END_OF_STREAM_MAGIC_1);
            writer.writeBits(footer, 24, END_OF_STREAM_MAGIC_2);
            writer.writeInt(footer, streamCRC);
            writer.flush(footer);
        } finally {
            blockCompressor = null;
        }
        return ctx.writeAndFlush(footer);
    }

    private ChannelHandlerContext ctx() {
        ChannelHandlerContext ctx = this.ctx;
        if (ctx == null) {
            throw new IllegalStateException("not added to a pipeline");
        }
        return ctx;
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
        this.ctx = ctx;
    }
}
