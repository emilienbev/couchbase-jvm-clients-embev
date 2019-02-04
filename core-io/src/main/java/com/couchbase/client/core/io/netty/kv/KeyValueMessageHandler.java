/*
 * Copyright (c) 2018 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.core.io.netty.kv;

import com.couchbase.client.core.Core;
import com.couchbase.client.core.CoreContext;
import com.couchbase.client.core.cnc.EventBus;
import com.couchbase.client.core.cnc.events.io.ChannelClosedProactivelyEvent;
import com.couchbase.client.core.cnc.events.io.InvalidRequestDetectedEvent;
import com.couchbase.client.core.cnc.events.io.UnknownResponseReceivedEvent;
import com.couchbase.client.core.cnc.events.io.UnsupportedResponseTypeReceivedEvent;
import com.couchbase.client.core.config.ProposedBucketConfigContext;
import com.couchbase.client.core.endpoint.EndpointContext;
import com.couchbase.client.core.env.CompressionConfig;
import com.couchbase.client.core.io.IoContext;
import com.couchbase.client.core.msg.Response;
import com.couchbase.client.core.msg.ResponseStatus;
import com.couchbase.client.core.msg.kv.KeyValueRequest;
import com.couchbase.client.core.retry.RetryOrchestrator;
import com.couchbase.client.core.service.ServiceType;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.util.CharsetUtil;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.collection.IntObjectHashMap;
import io.netty.util.collection.IntObjectMap;

import java.util.List;

import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.body;

/**
 * This handler is responsible for writing KV requests and completing their associated responses
 * once they arrive.
 *
 * @since 2.0.0
 */
public class KeyValueMessageHandler extends ChannelDuplexHandler {

  /**
   * Stores the {@link CoreContext} for use.
   */
  private final EndpointContext endpointContext;

  /**
   * Holds all outstanding requests based on their opaque.
   */
  private final IntObjectMap<KeyValueRequest<Response>> writtenRequests;

  /**
   * Holds the start timestamps for the outstanding dispatched requests.
   */
  private final IntObjectMap<Long> writtenRequestDispatchTimings;

  /**
   * The compression config used for this handler.
   */
  private final CompressionConfig compressionConfig;

  /**
   * The event bus used to signal events.
   */
  private final EventBus eventBus;

  private final String bucketName;

  /**
   * Stores the current IO context.
   */
  private IoContext ioContext;

  /**
   * Stores the current opaque value.
   */
  private int opaque;

  private ChannelContext channelContext;

  /**
   * Creates a new {@link KeyValueMessageHandler}.
   *
   * @param endpointContext the parent core context.
   */
  public KeyValueMessageHandler(final EndpointContext endpointContext, final String bucketName) {
    this.endpointContext = endpointContext;
    this.writtenRequests = new IntObjectHashMap<>();
    this.writtenRequestDispatchTimings = new IntObjectHashMap<>();
    this.compressionConfig = endpointContext.environment().ioEnvironment().compressionConfig();
    this.eventBus = endpointContext.environment().eventBus();
    this.bucketName = bucketName;
  }

  /**
   * Actions to be performed when the channel becomes active.
   *
   * <p>Since the opaque is incremented in the handler below during bootstrap but now is
   * only modified in this handler, cache the reference since the attribute lookup is
   * more costly.</p>
   *
   * @param ctx the channel context.
   */
  @Override
  public void channelActive(final ChannelHandlerContext ctx) {
    ioContext = new IoContext(
      endpointContext,
      ctx.channel().localAddress(),
      ctx.channel().remoteAddress(),
      endpointContext.bucket()
    );

    opaque = Utils.opaque(ctx.channel(), false);

    List<ServerFeature> features = ctx.channel().attr(ChannelAttributes.SERVER_FEATURE_KEY).get();
    boolean compressionEnabled = features != null && features.contains(ServerFeature.SNAPPY);
    boolean collectionsEnabled = features != null && features.contains(ServerFeature.COLLECTIONS);
    boolean mutationTokensEnabled = features != null && features.contains(ServerFeature.MUTATION_SEQNO);
    channelContext = new ChannelContext(
      compressionEnabled ? compressionConfig : null,
      collectionsEnabled,
      mutationTokensEnabled,
      bucketName
    );

    ctx.fireChannelActive();
  }

  @Override
  @SuppressWarnings({"unchecked"})
  public void write(final ChannelHandlerContext ctx, final Object msg, final ChannelPromise promise) {
    if (msg instanceof KeyValueRequest) {
      KeyValueRequest<Response> request = (KeyValueRequest<Response>) msg;

      int nextOpaque;
      do {
        nextOpaque = ++opaque;
      } while (writtenRequests.containsKey(nextOpaque));

      writtenRequests.put(nextOpaque, request);
      ctx.write(request.encode(ctx.alloc(), nextOpaque, channelContext));
      writtenRequestDispatchTimings.put(nextOpaque, (Long) System.nanoTime());
    } else {
      eventBus.publish(new InvalidRequestDetectedEvent(ioContext, ServiceType.KV, msg));
      ctx.channel().close().addListener(f -> eventBus.publish(new ChannelClosedProactivelyEvent(
        ioContext,
        ChannelClosedProactivelyEvent.Reason.INVALID_REQUEST_DETECTED)
      ));
    }
  }

  @Override
  public void channelRead(final ChannelHandlerContext ctx, final Object msg) {
    if (msg instanceof ByteBuf) {
      decode((ByteBuf) msg);
    } else {
      ioContext.environment().eventBus().publish(
        new UnsupportedResponseTypeReceivedEvent(ioContext, msg)
      );
      ReferenceCountUtil.release(msg);
    }
  }

  /**
   * Main method to start dispatching the decode.
   *
   * @param response the response to decode and handle.
   */
  private void decode(final ByteBuf response) {
    int opaque = MemcacheProtocol.opaque(response);
    KeyValueRequest<Response> request = writtenRequests.remove(opaque);
    long start = writtenRequestDispatchTimings.remove(opaque);

    if (request == null) {
      byte[] packet = new byte[response.readableBytes()];
      response.readBytes(packet);
      ioContext.environment().eventBus().publish(
        new UnknownResponseReceivedEvent(ioContext, packet)
      );
    } else {
      request.context().dispatchLatency(System.nanoTime() - start);

      ResponseStatus status = MemcacheProtocol.decodeStatus(response);
      if (status == ResponseStatus.NOT_MY_VBUCKET) {
        RetryOrchestrator.retryImmediately(ioContext, request);
        body(response)
          .map(b -> b.toString(CharsetUtil.UTF_8).trim())
          .filter(c -> c.startsWith("{"))
          .ifPresent(c -> ioContext.core().configurationProvider().proposeBucketConfig(
            new ProposedBucketConfigContext(
              bucketName,
              c,
              request.context().dispatchedTo()
            )
          ));
      } else {
        Response decoded = request.decode(response, channelContext);
        request.succeed(decoded);
      }
    }

    ReferenceCountUtil.release(response);
  }

}
