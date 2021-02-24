package de.fraunhofer.fit.processors.customlistentcp;

/**
 * Created by liang on 09.03.2018.
 */
import org.apache.commons.io.IOUtils;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.util.listen.dispatcher.AsyncChannelDispatcher;
import org.apache.nifi.processor.util.listen.dispatcher.SocketChannelAttachment;
import org.apache.nifi.processor.util.listen.event.Event;
import org.apache.nifi.processor.util.listen.event.EventFactory;
import org.apache.nifi.processor.util.listen.event.EventFactoryUtil;
import org.apache.nifi.processor.util.listen.handler.socket.SocketChannelHandler;
import org.apache.nifi.processor.util.listen.response.socket.SSLSocketChannelResponder;
import org.apache.nifi.processor.util.listen.response.socket.SocketChannelResponder;
import org.apache.nifi.remote.io.socket.ssl.SSLSocketChannel;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.nio.charset.Charset;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

/**
 * Wraps a SocketChannel with an SSLSocketChannel for receiving messages over TLS.
 */
public class CustomSSLSocketChannelHandler<E extends Event<SocketChannel>> extends SocketChannelHandler<E> {

    private final ByteArrayOutputStream currBytes = new ByteArrayOutputStream(4096);

    private boolean keepInMsgLenInfo;
    private int currDelimeterByteIndex;
    
    private final int msgLenInfo = 4;

    public CustomSSLSocketChannelHandler(final SelectionKey key,
                                   final AsyncChannelDispatcher dispatcher,
                                   final Charset charset,
                                   final EventFactory<E> eventFactory,
                                   final BlockingQueue<E> events,
                                   final ComponentLog logger,
                                   final boolean keepInMsgLenInfo) {
        super(key, dispatcher, charset, eventFactory, events, logger);
        this.keepInMsgLenInfo = keepInMsgLenInfo;
        this.currDelimeterByteIndex = 0;
    }

    @Override
    public void run() {
        boolean eof = false;
        SSLSocketChannel sslSocketChannel = null;
        try {
            int bytesRead;
            final SocketChannel socketChannel = (SocketChannel) key.channel();
            final SocketChannelAttachment attachment = (SocketChannelAttachment) key.attachment();

            // get the SSLSocketChannel from the attachment
            sslSocketChannel = attachment.getSslSocketChannel();

            // SSLSocketChannel deals with byte[] so ByteBuffer isn't used here, but we'll use the size to create a new byte[]
            final ByteBuffer socketBuffer = attachment.getByteBuffer();
            byte[] socketBufferArray = new byte[socketBuffer.limit()];

            // read until no more data
            try {
                while ((bytesRead = sslSocketChannel.read(socketBufferArray)) > 0) {
                    processBuffer(sslSocketChannel, socketChannel, bytesRead, socketBufferArray);
                    logger.debug("bytes read from sslSocketChannel {}", new Object[]{bytesRead});
                }
            } catch (SocketTimeoutException ste) {
                // SSLSocketChannel will throw this exception when 0 bytes are read and the timeout threshold
                // is exceeded, we don't want to close the connection in this case
                bytesRead = 0;
            }

            // Check for closed socket
            if( bytesRead < 0 ){
                eof = true;
                logger.debug("Reached EOF, closing connection");
            } else {
                logger.debug("No more data available, returning for selection");
            }
        } catch (ClosedByInterruptException | InterruptedException e) {
            logger.debug("read loop interrupted, closing connection");
            // Treat same as closed socket
            eof = true;
        } catch (ClosedChannelException e) {
            // ClosedChannelException doesn't have a message so handle it separately from IOException
            logger.error("Error reading from channel due to channel being closed", e);
            // Treat same as closed socket
            eof = true;
        } catch (IOException e) {
            logger.error("Error reading from channel due to {}", new Object[] {e.getMessage()}, e);
            // Treat same as closed socket
            eof = true;
        } finally {
            if(eof == true) {
                IOUtils.closeQuietly(sslSocketChannel);
                dispatcher.completeConnection(key);
            } else {
                dispatcher.addBackForSelection(key);
            }
        }
    }

    
    private static int byteArrayToLeInt(byte[] b) {
        final ByteBuffer bb = ByteBuffer.wrap(b);
        bb.order(ByteOrder.LITTLE_ENDIAN);
        return bb.getInt();
    }
    
    
    /**
     * Process the contents of the buffer. Give sub-classes a chance to override this behavior.
     *
     * @param sslSocketChannel the channel the data was read from
     * @param socketChannel the socket channel being wrapped by sslSocketChannel
     * @param bytesRead the number of bytes read
     * @param buffer the buffer to process
     * @throws InterruptedException thrown if interrupted while queuing events
     */
    protected void processBuffer(final SSLSocketChannel sslSocketChannel, final SocketChannel socketChannel,
                                 final int bytesRead, final byte[] buffer) throws InterruptedException, IOException {
    	
        final InetAddress sender = socketChannel.socket().getInetAddress();
        
        int currBytesRead = 0;
        while(currBytesRead < bytesRead) {
            
	        byte[] msgLengthArr = new byte[this.msgLenInfo];
	        System.arraycopy(buffer, currBytesRead, msgLengthArr, 0, msgLengthArr.length);
	        currBytesRead += msgLengthArr.length;
	        int msgLength = byteArrayToLeInt(msgLengthArr);
	        
	        byte[] message = new byte[msgLength];
	        System.arraycopy(buffer, currBytesRead, message, 0, message.length);
	        currBytesRead += message.length;
	        
	        byte[] final_message = null;
	        if(this.keepInMsgLenInfo) {
	        	final_message = new byte[msgLengthArr.length + message.length];
	        	System.arraycopy(msgLengthArr, 0, final_message, 0, msgLengthArr.length);
	        	System.arraycopy(message, 0, final_message, msgLengthArr.length, message.length);
	        }
	        else {
	        	final_message = message;
	        }
	        
            final SSLSocketChannelResponder response = new SSLSocketChannelResponder(socketChannel, sslSocketChannel);
	        final Map<String, String> metadata = EventFactoryUtil.createMapWithSender(sender.toString());
	        final E event = eventFactory.create(final_message, metadata, response);
	        events.offer(event);
	
	
	        
        }

    }

    @Override
    public byte getDelimiter() {
        return '\n';
    }

}
