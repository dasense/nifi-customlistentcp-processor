package de.fraunhofer.fit.processors.customlistentcp;

/**
 * Created by liang on 09.03.2018.
 */
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.util.listen.dispatcher.AsyncChannelDispatcher;
import org.apache.nifi.processor.util.listen.event.Event;
import org.apache.nifi.processor.util.listen.event.EventFactory;
import org.apache.nifi.processor.util.listen.handler.ChannelHandler;
import org.apache.nifi.processor.util.listen.handler.ChannelHandlerFactory;

import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.nio.charset.Charset;
import java.util.concurrent.BlockingQueue;

/**
 * Default factory for creating socket channel handlers.
 */
public class CustomSocketChannelHandlerFactory<E extends Event<SocketChannel>> implements ChannelHandlerFactory<E, AsyncChannelDispatcher> {


    private boolean keepInMsgLenInfo;

    public CustomSocketChannelHandlerFactory(boolean keepInMsgLenInfo) {
        this.keepInMsgLenInfo = keepInMsgLenInfo;
    }

    @Override
    public ChannelHandler<E, AsyncChannelDispatcher> createHandler(final SelectionKey key,
                                                                   final AsyncChannelDispatcher dispatcher,
                                                                   final Charset charset,
                                                                   final EventFactory<E> eventFactory,
                                                                   final BlockingQueue<E> events,
                                                                   final ComponentLog logger) {
        return new CustomSocketChannelHandler<>(key, dispatcher, charset, eventFactory, events, logger, keepInMsgLenInfo);
    }

    @Override
    public ChannelHandler<E, AsyncChannelDispatcher> createSSLHandler(final SelectionKey key,
                                                                      final AsyncChannelDispatcher dispatcher,
                                                                      final Charset charset,
                                                                      final EventFactory<E> eventFactory,
                                                                      final BlockingQueue<E> events,
                                                                      final ComponentLog logger) {
        return new CustomSSLSocketChannelHandler<>(key, dispatcher, charset, eventFactory, events, logger, keepInMsgLenInfo);
    }
}
