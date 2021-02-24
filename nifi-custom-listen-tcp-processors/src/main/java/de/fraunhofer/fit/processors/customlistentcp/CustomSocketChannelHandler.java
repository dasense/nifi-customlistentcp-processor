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
import org.apache.nifi.processor.util.listen.response.socket.SocketChannelResponder;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.nio.charset.Charset;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.nio.ByteOrder;


/**
 * Reads from the given SocketChannel into the provided buffer. If the given delimiter is found, the data
 * read up to that point is queued for processing.
 */
public class CustomSocketChannelHandler<E extends Event<SocketChannel>> extends SocketChannelHandler<E> {

    private final ByteArrayOutputStream currBytes = new ByteArrayOutputStream(4096);
    private boolean keepInMsgLenInfo;
    private int currDelimeterByteIndex;
    
    private final int msgLenInfo = 4;

    public CustomSocketChannelHandler(final SelectionKey key,
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
        SocketChannel socketChannel = null;

        try {
            int bytesRead;
            socketChannel = (SocketChannel) key.channel();

            final SocketChannelAttachment attachment = (SocketChannelAttachment) key.attachment();
            final ByteBuffer socketBuffer = attachment.getByteBuffer();

            // read until the buffer is full
            while ((bytesRead = socketChannel.read(socketBuffer)) > 0) {
                // prepare byte buffer for reading
                socketBuffer.flip();
                // mark the current position as start, in case of partial message read
                socketBuffer.mark();
                // process the contents that have been read into the buffer
                processBuffer(socketChannel, socketBuffer);

                // Preserve bytes in buffer for next call to run
                // NOTE: This code could benefit from the  two ByteBuffer read calls to avoid
                // this compact for higher throughput
                socketBuffer.reset();
                socketBuffer.compact();
                logger.debug("bytes read {}", new Object[]{bytesRead});
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
                IOUtils.closeQuietly(socketChannel);
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
     * Process the contents that have been read into the buffer. Allow sub-classes to override this behavior.
     *
     * @param socketChannel the channel the data was read from
     * @param socketBuffer the buffer the data was read into
     * @throws InterruptedException if interrupted when queuing events
     */
    protected void processBuffer(final SocketChannel socketChannel, final ByteBuffer socketBuffer) throws InterruptedException, IOException {
      
        final InetAddress sender = socketChannel.socket().getInetAddress();

        //at least the length info of the next message can be extracted
        while(socketBuffer.remaining() > this.msgLenInfo) {
        	
        	
	        byte[] msgLengthArr = new byte[this.msgLenInfo];
	        socketBuffer.get(msgLengthArr, 0, msgLengthArr.length);
	        int msgLength = byteArrayToLeInt(msgLengthArr);
	        
	        //check if we the whole message is contained in the buffer. if not, leave the function
	        if(socketBuffer.remaining() < msgLength) {
	        	//reset the position s.t. the buffer points to the start of the next message
	        	socketBuffer.position(socketBuffer.position() - this.msgLenInfo);
	        	socketBuffer.mark();
	        	return;
	        }
	        
	        byte[] message = new byte[msgLength];
	        socketBuffer.get(message,0,msgLength);
	        
	        byte[] final_message = null;
	        if(this.keepInMsgLenInfo) {
	        	final_message = new byte[msgLengthArr.length + message.length];
	        	System.arraycopy(msgLengthArr, 0, final_message, 0, msgLengthArr.length);
	        	System.arraycopy(message, 0, final_message, msgLengthArr.length, message.length);
	        }
	        else {
	        	final_message = message;
	        }
	        
	        final SocketChannelResponder response = new SocketChannelResponder(socketChannel);
	        final Map<String, String> metadata = EventFactoryUtil.createMapWithSender(sender.toString());
	        final E event = eventFactory.create(final_message, metadata, response);
	        events.offer(event);
	
	
	        // Mark this as the start of the next message
	        socketBuffer.mark();
        }
    }
    

    @Override
    public byte getDelimiter() {
        return '\n';
    }

}

