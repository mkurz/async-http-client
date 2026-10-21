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

import com.aayushatharva.brotli4j.encoder.Encoder;
import com.github.luben.zstd.Zstd;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.DecoderResult;
import io.netty.handler.codec.compression.Brotli;
import io.netty.handler.codec.compression.DecompressionException;
import io.netty.handler.codec.compression.SnappyFrameEncoder;
import io.netty.handler.codec.http.DefaultHttpContent;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.DefaultLastHttpContent;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.util.ReferenceCountUtil;
import org.asynchttpclient.AsyncCompletionHandlerBase;
import org.asynchttpclient.RequestBuilder;
import org.asynchttpclient.netty.NettyResponseBodyControl;
import org.asynchttpclient.netty.NettyResponseFuture;
import org.asynchttpclient.netty.channel.Channels;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.GZIPOutputStream;

import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link Http1ContentDecompressor} on an {@link EmbeddedChannel}, so suspension, channel closure
 * and handler removal can be driven deterministically and reference counts checked exactly.
 */
public class Http1ContentDecompressorTest {

    private static final byte[] TEXT = "the quick brown fox jumps over the lazy dog\n".repeat(2_000)
            .getBytes(StandardCharsets.US_ASCII);
    private static final byte[] OTHER_TEXT = "pack my box with five dozen liquor jugs\n".repeat(2_000)
            .getBytes(StandardCharsets.US_ASCII);
    private static final int ZEROS_SIZE = 8 * 1024 * 1024;
    private static final byte[] ZEROS_GZIP = gzip(new byte[ZEROS_SIZE]);

    private static byte[] gzip(byte[] payload) {
        return compress(payload, GZIPOutputStream::new);
    }

    private interface Compressor {
        OutputStream wrap(OutputStream out) throws IOException;
    }

    private static byte[] compress(byte[] payload, Compressor compressor) {
        ByteArrayOutputStream compressed = new ByteArrayOutputStream();
        try (OutputStream out = compressor.wrap(compressed)) {
            out.write(payload);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
        return compressed.toByteArray();
    }

    private static byte[] concat(byte[]... arrays) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        for (byte[] array : arrays) {
            out.writeBytes(array);
        }
        return out.toByteArray();
    }

    private static byte[] compress(String contentEncoding, byte[] payload) {
        switch (contentEncoding) {
            case "gzip":
                return gzip(payload);
            case "deflate":
                return compress(payload, DeflaterOutputStream::new);
            case "x-deflate":
                return compress(payload,
                        out -> new DeflaterOutputStream(out, new Deflater(Deflater.DEFAULT_COMPRESSION, true)));
            case "snappy":
                return snappy(payload);
            case "zstd":
                return Zstd.compress(payload);
            case "br":
                try {
                    Brotli.ensureAvailability();
                    return Encoder.compress(payload);
                } catch (Throwable e) {
                    throw new IllegalStateException(e);
                }
            default:
                throw new IllegalArgumentException(contentEncoding);
        }
    }

    private static byte[] snappy(byte[] payload) {
        EmbeddedChannel encoder = new EmbeddedChannel(new SnappyFrameEncoder());
        encoder.writeOutbound(Unpooled.wrappedBuffer(payload));
        encoder.finish();
        ByteArrayOutputStream compressed = new ByteArrayOutputStream();
        ByteBuf buf;
        while ((buf = encoder.readOutbound()) != null) {
            compressed.writeBytes(toBytes(buf));
            buf.release();
        }
        return compressed.toByteArray();
    }

    private static byte[] toBytes(ByteBuf buf) {
        byte[] bytes = new byte[buf.readableBytes()];
        buf.getBytes(buf.readerIndex(), bytes);
        return bytes;
    }

    private static HttpResponse response(String contentEncoding, int contentLength) {
        HttpResponse response = new DefaultHttpResponse(HTTP_1_1, HttpResponseStatus.OK);
        if (contentEncoding != null) {
            response.headers().set(HttpHeaderNames.CONTENT_ENCODING, contentEncoding);
        }
        response.headers().set(HttpHeaderNames.CONTENT_LENGTH, contentLength);
        return response;
    }

    /** Collects what the decompressor hands on, releasing each message. */
    private static final class Body {
        final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        final List<Object> messages = new ArrayList<>();
        int parts;
        int maxPart;
        boolean last;

        Body drain(EmbeddedChannel channel) {
            Object msg;
            while ((msg = channel.readInbound()) != null) {
                assertFalse(last, "nothing may follow the LastHttpContent");
                if (msg instanceof HttpContent) {
                    ByteBuf content = ((HttpContent) msg).content();
                    if (content.isReadable()) {
                        parts++;
                        maxPart = Math.max(maxPart, content.readableBytes());
                        bytes.writeBytes(toBytes(content));
                    }
                    last = msg instanceof LastHttpContent;
                }
                messages.add(msg instanceof HttpContent ? ((HttpContent) msg).getClass() : msg);
                ReferenceCountUtil.release(msg);
            }
            return this;
        }
    }

    /** Writes the body in fragments of {@code fragmentSize} bytes, as separate reads would deliver it. */
    private static Body decompressFragmented(String contentEncoding, byte[] compressed, int fragmentSize) {
        EmbeddedChannel channel = new EmbeddedChannel(new Http1ContentDecompressor(false, 0));
        channel.writeInbound(response(contentEncoding, compressed.length));
        for (int offset = 0; offset < compressed.length; offset += fragmentSize) {
            int length = Math.min(fragmentSize, compressed.length - offset);
            channel.writeInbound(new DefaultHttpContent(Unpooled.wrappedBuffer(compressed, offset, length)));
        }
        channel.writeInbound(LastHttpContent.EMPTY_LAST_CONTENT);
        Body body = new Body().drain(channel);
        assertFalse(channel.finishAndReleaseAll());
        return body;
    }

    /** Asserts that decompressing {@code compressed} fails, and returns the failure. */
    private static DecompressionException decompressionFailure(String contentEncoding, byte[] compressed,
                                                               long maxDecompressedBytes) {
        EmbeddedChannel channel = new EmbeddedChannel(new Http1ContentDecompressor(false, maxDecompressedBytes));
        channel.writeInbound(response(contentEncoding, compressed.length));
        ByteBuf input = Unpooled.wrappedBuffer(compressed);
        DecompressionException failure = assertThrows(DecompressionException.class,
                () -> channel.writeInbound(new DefaultLastHttpContent(input)));
        assertEquals(0, input.refCnt());
        channel.finishAndReleaseAll();
        return failure;
    }

    private static Body decompress(String contentEncoding, byte[] compressed, boolean keepEncodingHeader) {
        EmbeddedChannel channel = new EmbeddedChannel(new Http1ContentDecompressor(keepEncodingHeader, 0));
        channel.writeInbound(response(contentEncoding, compressed.length));
        channel.writeInbound(new DefaultLastHttpContent(Unpooled.wrappedBuffer(compressed)));
        Body body = new Body().drain(channel);
        assertFalse(channel.finishAndReleaseAll());
        return body;
    }

    @Test
    public void decompressesEveryContentCodingOfNettysDecompressor() throws Throwable {
        assertArrayEquals(TEXT, decompress("gzip", gzip(TEXT), false).bytes.toByteArray());
        assertArrayEquals(TEXT, decompress("X-GZIP", gzip(TEXT), false).bytes.toByteArray());
        assertArrayEquals(TEXT, decompress("deflate", compress(TEXT, DeflaterOutputStream::new), false)
                .bytes.toByteArray());
        assertArrayEquals(TEXT, decompress("x-deflate", compress(TEXT,
                out -> new DeflaterOutputStream(out, new Deflater(Deflater.DEFAULT_COMPRESSION, true))), false)
                .bytes.toByteArray(), "deflate without the zlib wrapper is accepted as Netty accepts it");
        assertArrayEquals(TEXT, decompress("snappy", snappy(TEXT), false).bytes.toByteArray());
        assertArrayEquals(TEXT, decompress("zstd", Zstd.compress(TEXT), false).bytes.toByteArray());
        Brotli.ensureAvailability();
        assertArrayEquals(TEXT, decompress("br", Encoder.compress(TEXT), false).bytes.toByteArray());
    }

    @Test
    public void rewritesHeadersLikeNettysDecompressor() {
        EmbeddedChannel channel = new EmbeddedChannel(new Http1ContentDecompressor(false, 0));
        channel.writeInbound(response("GZip", 10));
        HttpResponse decoded = channel.readInbound();
        assertNull(decoded.headers().get(HttpHeaderNames.CONTENT_ENCODING));
        assertNull(decoded.headers().get(HttpHeaderNames.CONTENT_LENGTH));
        assertEquals("chunked", decoded.headers().get(HttpHeaderNames.TRANSFER_ENCODING));
        assertFalse(channel.finishAndReleaseAll());

        channel = new EmbeddedChannel(new Http1ContentDecompressor(true, 0));
        channel.writeInbound(response("GZip", 10));
        decoded = channel.readInbound();
        assertEquals("GZip", decoded.headers().get(HttpHeaderNames.CONTENT_ENCODING));
        assertNull(decoded.headers().get(HttpHeaderNames.CONTENT_LENGTH));
        assertFalse(channel.finishAndReleaseAll());
    }

    @Test
    public void passesUnsupportedCodingsThroughUntouched() {
        EmbeddedChannel channel = new EmbeddedChannel(new Http1ContentDecompressor(false, 0));
        HttpResponse response = response("compress", 3);
        channel.writeInbound(response);
        ByteBuf content = Unpooled.wrappedBuffer(new byte[]{1, 2, 3});
        LastHttpContent last = new DefaultLastHttpContent(content);
        channel.writeInbound(last);
        assertSame(response, channel.readInbound());
        assertEquals("compress", response.headers().get(HttpHeaderNames.CONTENT_ENCODING));
        assertEquals("3", response.headers().get(HttpHeaderNames.CONTENT_LENGTH));
        assertSame(last, channel.readInbound());
        last.release();
        assertFalse(channel.finishAndReleaseAll());
    }

    @Test
    public void bodilessCompressedResponsesEndNormally() {
        // HEAD, 204 and 304 responses routinely carry Content-Encoding without a body.
        Body body = decompress("gzip", new byte[0], false);
        assertEquals(0, body.parts);
        assertTrue(body.last);
    }

    @Test
    public void trailersAndDecoderFailuresFollowTheBody() {
        EmbeddedChannel channel = new EmbeddedChannel(new Http1ContentDecompressor(false, 0));
        channel.writeInbound(response("gzip", 0));
        byte[] gzip = gzip(TEXT);
        channel.writeInbound(new DefaultHttpContent(Unpooled.wrappedBuffer(gzip, 0, gzip.length / 2)));
        LastHttpContent last = new DefaultLastHttpContent(
                Unpooled.wrappedBuffer(gzip, gzip.length / 2, gzip.length - gzip.length / 2));
        last.trailingHeaders().set("x-trailer", "done");
        IllegalStateException failure = new IllegalStateException("bad chunk");
        last.setDecoderResult(DecoderResult.failure(failure));
        channel.writeInbound(last);

        assertInstanceOf(HttpResponse.class, channel.readInbound());
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        Object msg;
        LastHttpContent trailer = null;
        while ((msg = channel.readInbound()) != null) {
            assertNull(trailer, "the trailer must come last");
            HttpContent content = (HttpContent) msg;
            bytes.writeBytes(toBytes(content.content()));
            if (msg instanceof LastHttpContent) {
                trailer = (LastHttpContent) msg;
            }
            content.release();
        }
        assertArrayEquals(TEXT, bytes.toByteArray());
        assertEquals("done", trailer.trailingHeaders().get("x-trailer"));
        assertSame(failure, trailer.decoderResult().cause(), "a framing failure must not be turned into success");
        assertFalse(channel.finishAndReleaseAll());
    }

    @Test
    public void continueResponsesPassThroughUntouched() {
        EmbeddedChannel channel = new EmbeddedChannel(new Http1ContentDecompressor(false, 0));
        HttpResponse interim = new DefaultHttpResponse(HTTP_1_1, HttpResponseStatus.CONTINUE);
        interim.headers().set(HttpHeaderNames.CONTENT_ENCODING, "gzip");
        channel.writeInbound(interim);
        channel.writeInbound(LastHttpContent.EMPTY_LAST_CONTENT);
        assertSame(interim, channel.readInbound());
        assertEquals("gzip", interim.headers().get(HttpHeaderNames.CONTENT_ENCODING));
        assertSame(LastHttpContent.EMPTY_LAST_CONTENT, channel.readInbound());

        channel.writeInbound(response("gzip", 0));
        channel.writeInbound(new DefaultLastHttpContent(Unpooled.wrappedBuffer(gzip(TEXT))));
        assertInstanceOf(HttpResponse.class, channel.readInbound());
        assertArrayEquals(TEXT, new Body().drain(channel).bytes.toByteArray());
        assertFalse(channel.finishAndReleaseAll());
    }

    /** A channel whose response is suspended by the handler after every body part, as a demand-driven consumer does. */
    private static final class SuspendedExchange {
        final EmbeddedChannel channel;
        final NettyResponseBodyControl control;
        final List<ByteBuf> input = new ArrayList<>();
        final ByteBuf compressed;
        final boolean suspendedBeforeBody;

        SuspendedExchange(long maxDecompressedBytes) {
            this(maxDecompressedBytes, false);
        }

        SuspendedExchange(long maxDecompressedBytes, boolean suspendBeforeBody) {
            this("gzip", ZEROS_GZIP, ZEROS_GZIP.length, maxDecompressedBytes, suspendBeforeBody, true);
        }

        /** Writes the body in fragments of {@code fragmentSize} bytes, the last one as the {@link LastHttpContent}. */
        SuspendedExchange(String contentEncoding, byte[] body, int fragmentSize, long maxDecompressedBytes,
                          boolean suspendBeforeBody, boolean autoRead) {
            suspendedBeforeBody = suspendBeforeBody;
            channel = new EmbeddedChannel();
            channel.config().setAutoRead(autoRead);
            NettyResponseFuture<?> future = new NettyResponseFuture<>(
                    new RequestBuilder().setUrl("http://localhost/").build(), new AsyncCompletionHandlerBase(),
                    null, 0, null, null, null);
            Channels.setAttribute(channel, future);
            control = NettyResponseBodyControl.create(future, channel, () -> {
            }, ignored -> {
            });
            channel.pipeline().addLast(new Http1ContentDecompressor(false, maxDecompressedBytes),
                    new ChannelInboundHandlerAdapter() {
                        @Override
                        public void channelRead(ChannelHandlerContext ctx, Object msg) {
                            if (msg instanceof HttpContent) {
                                control.suspend();
                            }
                            ctx.fireChannelRead(msg);
                        }
                    });
            channel.writeInbound(response(contentEncoding, body.length));
            ReferenceCountUtil.release(channel.readInbound());
            if (suspendBeforeBody) {
                control.suspend();
            }
            for (int offset = 0; offset < body.length; offset += fragmentSize) {
                int length = Math.min(fragmentSize, body.length - offset);
                ByteBuf fragment = Unpooled.wrappedBuffer(body, offset, length);
                input.add(fragment);
                channel.writeInbound(offset + length < body.length ? new DefaultHttpContent(fragment)
                        : new DefaultLastHttpContent(fragment));
            }
            compressed = input.get(0);
        }

        /** Resumes until the body has ended, checking that each resume delivers exactly one more part. */
        Body resumeToTheEnd() {
            Body body = new Body().drain(channel);
            assertEquals(suspendedBeforeBody ? 0 : 1, body.parts, "nothing more may arrive while suspended");
            for (int resumes = 0; !body.last; resumes++) {
                assertTrue(resumes < 10_000, "the body must end");
                int before = body.parts;
                control.resume();
                body.drain(channel);
                assertTrue(body.parts == before + 1 || body.last && body.parts == before,
                        "each resume must deliver exactly one more part");
            }
            assertTrue(body.maxPart <= Http1ContentDecompressor.MAX_PART_SIZE);
            for (ByteBuf fragment : input) {
                assertEquals(0, fragment.refCnt());
            }
            assertFalse(channel.finishAndReleaseAll());
            return body;
        }
    }

    @Test
    public void suspensionBeforeTheBodyHoldsEveryPartBack() {
        SuspendedExchange exchange = new SuspendedExchange(0, true);
        Body body = new Body().drain(exchange.channel);
        assertEquals(0, body.parts, "a response suspended from onResponseBodyStart must not receive any part");

        exchange.control.resume();
        body.drain(exchange.channel);
        assertEquals(1, body.parts);
        exchange.channel.pipeline().remove(Http1ContentDecompressor.class);
        assertEquals(0, exchange.compressed.refCnt());
        assertFalse(exchange.channel.finishAndReleaseAll());
    }

    @Test
    public void suspensionHoldsInputUntilReadIsRequested() {
        SuspendedExchange exchange = new SuspendedExchange(0);
        Body body = new Body().drain(exchange.channel);
        assertEquals(1, body.parts, "decompression must stop at the part that suspended the response");

        int resumes = 0;
        while (!body.last) {
            int before = body.parts;
            exchange.control.resume();
            resumes++;
            body.drain(exchange.channel);
            assertEquals(before + 1, body.parts, "each read request must inflate exactly one more part");
        }
        assertEquals(ZEROS_SIZE, body.bytes.size());
        assertTrue(body.maxPart <= Http1ContentDecompressor.MAX_PART_SIZE);
        assertEquals(resumes + 1, body.parts);
        assertEquals(0, exchange.compressed.refCnt());
        assertFalse(exchange.channel.finishAndReleaseAll());
    }

    @Test
    public void channelClosureHandsOnHeldInputBeforeChannelInactive() {
        SuspendedExchange exchange = new SuspendedExchange(0);
        List<Object> events = new ArrayList<>();
        exchange.channel.pipeline().addLast(new ChannelInboundHandlerAdapter() {
            @Override
            public void channelInactive(ChannelHandlerContext ctx) {
                events.add("inactive");
            }
        });
        Body body = new Body().drain(exchange.channel);
        assertEquals(1, body.parts);

        exchange.channel.pipeline().fireChannelInactive();
        body.drain(exchange.channel);
        assertTrue(body.last);
        assertEquals(ZEROS_SIZE, body.bytes.size());
        assertEquals(List.of("inactive"), events);
        assertEquals(0, exchange.compressed.refCnt());
        exchange.channel.finishAndReleaseAll();
    }

    @Test
    public void removalReleasesHeldInput() {
        SuspendedExchange exchange = new SuspendedExchange(0);
        Body body = new Body().drain(exchange.channel);
        assertEquals(1, body.parts);
        assertEquals(1, exchange.compressed.refCnt(), "the compressed input is held while suspended");

        exchange.channel.pipeline().remove(Http1ContentDecompressor.class);
        assertEquals(0, exchange.compressed.refCnt());
        assertFalse(exchange.channel.finishAndReleaseAll());
    }

    @Test
    public void limitExceededWhileResumingFailsTheExchangeAndReleasesInput() {
        SuspendedExchange exchange = new SuspendedExchange(1024 * 1024);
        Body body = new Body().drain(exchange.channel);
        DecompressionException failure = null;
        while (failure == null) {
            assertFalse(body.last);
            int before = body.parts;
            exchange.control.resume();
            body.drain(exchange.channel);
            try {
                exchange.channel.checkException();
            } catch (DecompressionException e) {
                failure = e;
            }
            assertTrue(failure != null || body.parts > before, "each read request must inflate more or fail");
        }
        assertTrue(failure.getMessage().contains("maximum decompressed size of 1048576 bytes"));
        assertTrue(body.bytes.size() <= 1024 * 1024);
        assertEquals(0, exchange.compressed.refCnt());

        // The rest of the failed body is dropped; only its end is still passed on.
        long delivered = body.bytes.size();
        exchange.channel.read();
        body.drain(exchange.channel);
        assertTrue(body.last);
        assertEquals(delivered, body.bytes.size());
        assertFalse(exchange.channel.finishAndReleaseAll());
    }

    /** One part made of many small decompressor outputs: one per gzip member of {@code memberSize} bytes. */
    private static ByteBuf mergedPart(int members, int memberSize) {
        byte[] member = gzip("x".repeat(memberSize).getBytes(StandardCharsets.US_ASCII));
        byte[] compressed = concat(Collections.nCopies(members, member).toArray(new byte[0][]));
        EmbeddedChannel channel = new EmbeddedChannel(new Http1ContentDecompressor(false, 0));
        channel.writeInbound(response("gzip", compressed.length));
        channel.writeInbound(new DefaultLastHttpContent(Unpooled.wrappedBuffer(compressed)));
        ReferenceCountUtil.release(channel.readInbound());
        HttpContent part = channel.readInbound();
        assertInstanceOf(LastHttpContent.class, channel.readInbound());
        assertFalse(channel.finishAndReleaseAll());
        return part.content();
    }

    @Test
    public void mergedPartIsAllocatedForWhatItHolds() {
        ByteBuf part = mergedPart(10, 100);
        try {
            assertEquals(1_000, part.readableBytes());
            assertTrue(part.capacity() <= 2 * part.readableBytes(),
                    "a small merged part must not take a whole part's buffer: " + part.capacity());
        } finally {
            part.release();
        }
    }

    @Test
    public void mergedPartGrowsUpToThePartSize() {
        ByteBuf part = mergedPart(40, 1_000);
        try {
            assertEquals(40_000, part.readableBytes());
            assertTrue(part.capacity() <= Http1ContentDecompressor.MAX_PART_SIZE);
            byte[] expected = "x".repeat(40_000).getBytes(StandardCharsets.US_ASCII);
            assertArrayEquals(expected, toBytes(part));
        } finally {
            part.release();
        }
    }

    @Test
    public void decompressesEveryMemberOfAConcatenatedGzipBody() {
        byte[] compressed = concat(gzip(TEXT), gzip(OTHER_TEXT));
        byte[] expected = concat(TEXT, OTHER_TEXT);
        assertArrayEquals(expected, decompress("gzip", compressed, false).bytes.toByteArray());
        assertArrayEquals(expected, decompressFragmented("x-gzip", compressed, 1).bytes.toByteArray());
        assertArrayEquals(expected, decompressFragmented("gzip", compressed, gzip(TEXT).length).bytes.toByteArray());
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void suspensionCarriesOverIntoTheNextGzipMember(boolean autoRead) {
        byte[] first = new byte[300 * 1024];
        byte[] second = "x".repeat(300 * 1024).getBytes(StandardCharsets.US_ASCII);
        SuspendedExchange exchange = new SuspendedExchange("gzip", concat(gzip(first), gzip(second)), 512, 0, true,
                autoRead);
        assertArrayEquals(concat(first, second), exchange.resumeToTheEnd().bytes.toByteArray());
    }

    @Test
    public void corruptLaterGzipMemberFailsTheBody() {
        // A member ends with the CRC-32 of its data and the data's length. Corrupt compressed data can instead look
        // like a stream that ends early, which is accepted like any truncated body, as Netty's decoder accepts it.
        byte[] second = gzip(OTHER_TEXT);
        byte[] corruptChecksum = second.clone();
        corruptChecksum[second.length - 8] ^= (byte) 0xFF;
        byte[] corruptLength = second.clone();
        corruptLength[second.length - 4] ^= (byte) 0xFF;
        assertTrue(decompressionFailure("gzip", concat(gzip(TEXT), corruptChecksum), 0).getMessage()
                .contains("CRC value mismatch"));
        assertTrue(decompressionFailure("gzip", concat(gzip(TEXT), corruptLength), 0).getMessage()
                .contains("Number of bytes mismatch"));
    }

    @Test
    public void bytesAfterTheCompressedStreamAreHandledAsNettysDecompressorHandlesThem() {
        byte[] junk = "not compressed at all".getBytes(StandardCharsets.US_ASCII);
        // After a gzip member, they would have to start another member.
        decompressionFailure("gzip", concat(gzip(TEXT), junk), 0);
        // Too few of them to make up a gzip member header, so the body ends before they are looked at.
        assertArrayEquals(TEXT, decompress("gzip", concat(gzip(TEXT), new byte[]{1, 2, 3}), false)
                .bytes.toByteArray());
        // A deflate stream cannot be continued, so they are dropped.
        assertArrayEquals(TEXT, decompress("deflate", concat(compress("deflate", TEXT), junk), false)
                .bytes.toByteArray());
    }

    @Test
    public void limitCountsEveryGzipMember() {
        DecompressionException failure = decompressionFailure("gzip", concat(gzip(TEXT), gzip(TEXT)),
                TEXT.length + 1);
        assertTrue(failure.getMessage().contains("maximum decompressed size of " + (TEXT.length + 1) + " bytes"));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void everyCodingDeliversBoundedPartsAcrossRepeatedSuspension(boolean autoRead) {
        // Incompressible, highly compressible and text data, so that both large and small decompressor output occurs.
        byte[] random = new byte[130_000];
        new Random(29).nextBytes(random);
        byte[] payload = concat(random, new byte[200_000], TEXT);
        for (String contentEncoding : List.of("gzip", "deflate", "x-deflate", "snappy", "zstd", "br")) {
            SuspendedExchange exchange = new SuspendedExchange(contentEncoding, compress(contentEncoding, payload),
                    4096, 0, true, autoRead);
            assertArrayEquals(payload, exchange.resumeToTheEnd().bytes.toByteArray(), contentEncoding);
        }
    }
}
