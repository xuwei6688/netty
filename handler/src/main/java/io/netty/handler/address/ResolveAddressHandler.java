/*
 * Copyright 2020 The Netty Project
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
package io.netty.handler.address;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.resolver.AddressResolver;
import io.netty.resolver.AddressResolverGroup;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.FutureListener;
import io.netty.util.concurrent.Promise;
import io.netty.util.concurrent.PromiseNotifier;
import io.netty.util.internal.ObjectUtil;

import java.net.SocketAddress;

/**
 * {@link ChannelHandler} which will resolve the {@link SocketAddress} that is passed to
 * {@link #connect(ChannelHandlerContext, SocketAddress, SocketAddress)} if it is not already resolved
 * and the {@link AddressResolver} supports the type of {@link SocketAddress}.
 */
@Sharable
public class ResolveAddressHandler implements ChannelHandler {

    private final AddressResolverGroup<? extends SocketAddress> resolverGroup;

    public ResolveAddressHandler(AddressResolverGroup<? extends SocketAddress> resolverGroup) {
        this.resolverGroup = ObjectUtil.checkNotNull(resolverGroup, "resolverGroup");
    }

    @Override
    public Future<Void> connect(final ChannelHandlerContext ctx, SocketAddress remoteAddress,
                          final SocketAddress localAddress)  {
        AddressResolver<? extends SocketAddress> resolver = resolverGroup.getResolver(ctx.executor());
        if (resolver.isSupported(remoteAddress) && !resolver.isResolved(remoteAddress)) {
            Promise<Void> promise = ctx.newPromise();
            resolver.resolve(remoteAddress).addListener((FutureListener<SocketAddress>) future -> {
                Throwable cause = future.cause();
                if (cause != null) {
                    promise.setFailure(cause);
                } else {
                    ctx.connect(future.getNow(), localAddress).addListener(new PromiseNotifier<>(promise));
                }
                ctx.pipeline().remove(ResolveAddressHandler.this);
            });
            return promise;
        } else {
            Future<Void> f = ctx.connect(remoteAddress, localAddress);
            ctx.pipeline().remove(this);
            return f;
        }
    }
}
