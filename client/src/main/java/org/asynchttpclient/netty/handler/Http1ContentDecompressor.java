/*
 *    Copyright (c) 2026 AsyncHttpClient Project. All rights reserved.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package org.asynchttpclient.netty.handler;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.compression.Brotli;
import io.netty.handler.codec.compression.BrotliDecompressor;
import io.netty.handler.codec.compression.DecompressionException;
import io.netty.handler.codec.compression.Decompressor;
import io.netty.handler.codec.compression.JdkZlibDecompressor;
import io.netty.handler.codec.compression.SnappyFrameDecompressor;
import io.netty.handler.codec.compression.ZlibWrapper;
import io.netty.handler.codec.compression.Zstd;
import io.netty.handler.codec.compression.ZstdDecompressor;
import io.netty.handler.codec.http.DefaultHttpContent;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.DefaultLastHttpContent;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.util.ReferenceCountUtil;
import org.asynchttpclient.netty.DiscardEvent;
import org.asynchttpclient.netty.NettyResponseBodyControl;
import org.asynchttpclient.netty.NettyResponseFuture;
import org.asynchttpclient.netty.channel.Channels;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayDeque;

/**
 * HTTP/1.1 content decompressor that inflates a response body only as fast as the response consumes it, and
 * bounds how far the body may inflate in total.
 * <p>
 * Netty's {@code HttpContentDecompressor} inflates each compressed chunk completely as soon as it is read.
 * Suspending a response through {@link org.asynchttpclient.ResponseBodyControl} stops further socket reads but
 * cannot stop that, so one socket read of a highly compressible body still became tens of megabytes of body parts
 * nobody had asked for. This handler uses Netty's pull-based {@link Decompressor} instead and hands the body on
 * one part of at most {@value #MAX_PART_SIZE} bytes at a time. Before each part it checks whether the response is
 * suspended. If it is, the compressed input stays here, together with every message that arrived after it, and
 * the next read request, such as the one {@code resume()} issues, inflates that input before it is passed on to
 * the transport. A suspension that takes effect on the event loop, as one made from a body callback does,
 * therefore stops decompression before the next part.
 * <p>
 * Closing the channel tears its pipeline down, so if the connection closes while input is held back, the rest of
 * it is inflated and handed on at once, as it was before.
 * <p>
 * The cumulative decompressed size of each response is bounded by {@code maxDecompressedBytes}; exceeding it
 * fails the exchange with a {@link DecompressionException}, the same exception type the HTTP/2 path uses. Only
 * what decompression produced is counted, so an unencoded response, which is passed through untouched, is never
 * failed for being large; its size is the caller's own choice, not a bomb.
 * <p>
 * Content codings, and the {@code Content-Encoding}, {@code Content-Length} and {@code Transfer-Encoding} header
 * rewrites, follow Netty's {@code HttpContentDecompressor}.
 * <p>
 * Netty still marks its {@link Decompressor} API as unstable while it migrates its codecs to it, so a Netty upgrade
 * may require changes here.
 */
public class Http1ContentDecompressor extends ChannelDuplexHandler {

    // Largest body part handed on at once. Larger decompressor output, such as zstd's when Netty's
    // io.netty.compression.defaultMaxForwardBytes is raised, is split.
    static final int MAX_PART_SIZE = 64 * 1024;

    private final boolean keepEncodingHeader;
    // Maximum cumulative decompressed bytes for one response; 0 disables the limit.
    private final long maxDecompressedBytes;

    // Inbound messages not yet handled, in arrival order. They queue up only behind a suspended response body.
    private final ArrayDeque<Object> pending = new ArrayDeque<>();
    // Null when the current response body is not compressed, or once decompressing it failed or became pointless.
    private @Nullable Decompressor decompressor;
    // Whether the current response body is compressed. Stays set after a failure so the rest of the body is dropped.
    private boolean decoding;
    // Output already taken from the decompressor that did not fit into the previous part.
    private @Nullable ByteBuf carriedOutput;
    // The terminating chunk of the current response, handed on once the rest of its body has been.
    private @Nullable LastHttpContent lastContent;
    private long decompressedBytes;
    private boolean continueResponse;
    private boolean draining;
    // Whether decompressed output is held back for a suspended response.
    private boolean paused;
    private boolean readPending;
    private boolean inputClosed;

    public Http1ContentDecompressor(boolean keepEncodingHeader, long maxDecompressedBytes) {
        this.keepEncodingHeader = keepEncodingHeader;
        this.maxDecompressedBytes = maxDecompressedBytes;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        pending.add(msg);
        if (!draining) {
            drain(ctx);
        }
    }

    @Override
    public void read(ChannelHandlerContext ctx) {
        if (draining || paused) {
            // Input that was already read is used up before the transport is asked for more.
            readPending = true;
            if (!draining) {
                drain(ctx);
            }
        } else {
            ctx.read();
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        // The pipeline is about to be torn down, so anything held back has to go now, even to a suspended response.
        inputClosed = true;
        try {
            if (paused && !draining) {
                drain(ctx);
            }
        } catch (Throwable cause) {
            ctx.fireExceptionCaught(cause);
        } finally {
            release();
        }
        ctx.fireChannelInactive();
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) {
        release();
    }

    private void drain(ChannelHandlerContext ctx) {
        draining = true;
        try {
            paused = !process(ctx);
        } finally {
            draining = false;
        }
        if (!paused && readPending) {
            readPending = false;
            ctx.read();
        }
    }

    /**
     * Hands on everything that can be handed on. Returns {@code false} if it stopped because the response is
     * suspended while it has decompressed output to deliver.
     */
    private boolean process(ChannelHandlerContext ctx) {
        try {
            for (;;) {
                Decompressor decompressor = this.decompressor;
                if (decompressor != null && hasOutput(decompressor)) {
                    Channel channel = ctx.channel();
                    if (isAbandoned(channel)) {
                        // The exchange is over; inflating the rest of its body would be wasted work.
                        closeDecompressor();
                        continue;
                    }
                    if (!inputClosed && isSuspended(channel)) {
                        return false;
                    }
                    ByteBuf part = nextPart(decompressor, ctx.alloc());
                    if (part != null) {
                        ctx.fireChannelRead(new DefaultHttpContent(part));
                    }
                } else if (lastContent != null) {
                    LastHttpContent last = lastContent;
                    lastContent = null;
                    endResponse();
                    ctx.fireChannelRead(last);
                } else {
                    Object msg = pending.poll();
                    if (msg == null) {
                        return true;
                    }
                    handle(ctx, msg);
                }
            }
        } catch (RuntimeException e) {
            // A corrupt or oversized body fails the exchange; the rest of it is dropped.
            closeDecompressor();
            throw e;
        }
    }

    private void handle(ChannelHandlerContext ctx, Object msg) {
        // A 100 Continue, and everything up to the LastHttpContent that ends it, passes through untouched, as with
        // Netty's HttpContentDecoder.
        if (continueResponse || msg instanceof HttpResponse && ((HttpResponse) msg).status().code() == 100) {
            continueResponse = !(msg instanceof LastHttpContent);
            ctx.fireChannelRead(msg);
            return;
        }

        if (msg instanceof HttpResponse) {
            HttpResponse response = (HttpResponse) msg;
            try {
                startResponse(response, ctx.alloc());
            } catch (RuntimeException e) {
                ReferenceCountUtil.release(msg);
                throw e;
            }
            if (!decoding || !(msg instanceof HttpContent)) {
                ctx.fireChannelRead(msg);
                return;
            }
            // Only the content of a response that carries its own content is decompressed.
            DefaultHttpResponse head = new DefaultHttpResponse(response.protocolVersion(), response.status(),
                    response.headers());
            head.setDecoderResult(response.decoderResult());
            ctx.fireChannelRead(head);
        }

        if (decoding && msg instanceof HttpContent) {
            decode((HttpContent) msg);
        } else {
            ctx.fireChannelRead(msg);
        }
    }

    private void startResponse(HttpResponse response, ByteBufAllocator alloc) {
        endResponse();
        HttpHeaders headers = response.headers();
        String contentEncoding = contentEncoding(headers);
        decompressor = newDecompressor(contentEncoding, alloc);
        if (contentEncoding == null || decompressor == null) {
            return;
        }

        decoding = true;
        decompressedBytes = 0;
        if (headers.contains(HttpHeaderNames.CONTENT_LENGTH)) {
            headers.remove(HttpHeaderNames.CONTENT_LENGTH);
            headers.set(HttpHeaderNames.TRANSFER_ENCODING, HttpHeaderValues.CHUNKED);
        }
        // Leaving Content-Encoding in place lets the caller see what the server sent, at the cost of a header that
        // no longer describes the body handed up. Opt-in via keepEncodingHeader.
        if (keepEncodingHeader) {
            headers.set(HttpHeaderNames.CONTENT_ENCODING, contentEncoding);
        } else {
            headers.remove(HttpHeaderNames.CONTENT_ENCODING);
        }
    }

    private void decode(HttpContent content) {
        try {
            Decompressor decompressor = this.decompressor;
            ByteBuf input = content.content();
            // Once the compressed stream is complete, further bytes are dropped, as Netty's decoders drop them. A gzip
            // stream is only complete at the end of the input, because another member may follow.
            if (decompressor != null && input.isReadable()
                    && decompressor.status() == Decompressor.Status.NEED_INPUT) {
                decompressor.addInput(input.retain());
            }
            if (content instanceof LastHttpContent) {
                lastContent = trailer((LastHttpContent) content);
            }
        } finally {
            content.release();
        }
    }

    private boolean hasOutput(Decompressor decompressor) {
        return carriedOutput != null || decompressor.status() == Decompressor.Status.NEED_OUTPUT;
    }

    /**
     * Takes up to {@link #MAX_PART_SIZE} bytes of output. Small output buffers are copied together so a highly
     * compressible body is not handed on in hundreds of tiny parts.
     */
    private @Nullable ByteBuf nextPart(Decompressor decompressor, ByteBufAllocator alloc) {
        ByteBuf part = carriedOutput;
        carriedOutput = null;
        if (part != null && part.readableBytes() > MAX_PART_SIZE) {
            return split(part);
        }
        boolean merged = false;
        try {
            while (decompressor.status() == Decompressor.Status.NEED_OUTPUT) {
                ByteBuf output = decompressor.takeOutput();
                int length = output.readableBytes();
                if (maxDecompressedBytes > 0 && (decompressedBytes += length) > maxDecompressedBytes) {
                    output.release();
                    throw new DecompressionException("HTTP/1.1 response body exceeds the maximum decompressed size of "
                            + maxDecompressedBytes + " bytes");
                }

                if (length == 0) {
                    output.release();
                } else if (part == null) {
                    if (length > MAX_PART_SIZE) {
                        return split(output);
                    }
                    part = output;
                } else if (part.readableBytes() + length > MAX_PART_SIZE) {
                    carriedOutput = output;
                    break;
                } else {
                    if (!merged) {
                        ByteBuf first = part;
                        part = alloc.heapBuffer(MAX_PART_SIZE);
                        part.writeBytes(first);
                        first.release();
                        merged = true;
                    }
                    part.writeBytes(output);
                    output.release();
                }
            }
        } catch (RuntimeException e) {
            ReferenceCountUtil.release(part);
            throw e;
        }
        return part;
    }

    /** Hands on the first {@link #MAX_PART_SIZE} bytes of {@code output} and carries the rest over. */
    private ByteBuf split(ByteBuf output) {
        ByteBuf part = output.readRetainedSlice(MAX_PART_SIZE);
        carriedOutput = output;
        return part;
    }

    private void endResponse() {
        closeDecompressor();
        decoding = false;
    }

    private void closeDecompressor() {
        if (carriedOutput != null) {
            carriedOutput.release();
            carriedOutput = null;
        }
        Decompressor decompressor = this.decompressor;
        if (decompressor != null) {
            this.decompressor = null;
            decompressor.close();
        }
    }

    private void release() {
        Object msg;
        while ((msg = pending.poll()) != null) {
            ReferenceCountUtil.release(msg);
        }
        lastContent = null;
        paused = false;
        endResponse();
    }

    private static boolean isSuspended(Channel channel) {
        Object attribute = Channels.getAttribute(channel);
        return attribute instanceof NettyResponseFuture
                && NettyResponseBodyControl.isSuspended((NettyResponseFuture<?>) attribute, channel);
    }

    private static boolean isAbandoned(Channel channel) {
        Object attribute = Channels.getAttribute(channel);
        return attribute == DiscardEvent.DISCARD
                || attribute instanceof NettyResponseFuture && ((NettyResponseFuture<?>) attribute).isDone();
    }

    private static @Nullable String contentEncoding(HttpHeaders headers) {
        String contentEncoding = headers.get(HttpHeaderNames.CONTENT_ENCODING);
        if (contentEncoding != null) {
            return contentEncoding.trim();
        }
        String transferEncoding = headers.get(HttpHeaderNames.TRANSFER_ENCODING);
        if (transferEncoding == null) {
            return null;
        }
        int comma = transferEncoding.indexOf(',');
        return (comma != -1 ? transferEncoding.substring(0, comma) : transferEncoding).trim();
    }

    private static @Nullable Decompressor newDecompressor(@Nullable String contentEncoding, ByteBufAllocator alloc) {
        if (contentEncoding == null) {
            return null;
        }
        if (HttpHeaderValues.GZIP.contentEqualsIgnoreCase(contentEncoding)
                || HttpHeaderValues.X_GZIP.contentEqualsIgnoreCase(contentEncoding)) {
            // A gzip body may consist of several members (RFC 1952, section 2.2). Netty's HttpContentDecompressor
            // decodes all of them, and so does this one.
            return JdkZlibDecompressor.builder().wrapper(ZlibWrapper.GZIP).decompressConcatenated(true)
                    .maxAllocation(MAX_PART_SIZE).build(alloc);
        }
        if (HttpHeaderValues.DEFLATE.contentEqualsIgnoreCase(contentEncoding)
                || HttpHeaderValues.X_DEFLATE.contentEqualsIgnoreCase(contentEncoding)) {
            return JdkZlibDecompressor.builder().wrapper(ZlibWrapper.ZLIB_OR_NONE).maxAllocation(MAX_PART_SIZE)
                    .build(alloc);
        }
        if (Brotli.isAvailable() && HttpHeaderValues.BR.contentEqualsIgnoreCase(contentEncoding)) {
            return BrotliDecompressor.builder().maxOutputChunkSize(MAX_PART_SIZE).build(alloc);
        }
        if (HttpHeaderValues.SNAPPY.contentEqualsIgnoreCase(contentEncoding)) {
            return SnappyFrameDecompressor.builder().build(alloc);
        }
        if (Zstd.isAvailable() && HttpHeaderValues.ZSTD.contentEqualsIgnoreCase(contentEncoding)) {
            return ZstdDecompressor.builder().build(alloc);
        }
        return null;
    }

    private static LastHttpContent trailer(LastHttpContent last) {
        if (last.trailingHeaders().isEmpty() && last.decoderResult().isSuccess()) {
            return LastHttpContent.EMPTY_LAST_CONTENT;
        }
        LastHttpContent trailer = new DefaultLastHttpContent(Unpooled.EMPTY_BUFFER, last.trailingHeaders());
        trailer.setDecoderResult(last.decoderResult());
        return trailer;
    }
}
