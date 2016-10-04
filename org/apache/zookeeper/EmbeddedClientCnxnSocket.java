package org.apache.zookeeper;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;

import org.apache.zookeeper.ClientCnxn.EndOfStreamException;
import org.apache.zookeeper.ClientCnxn.Packet;
import org.apache.zookeeper.ZooDefs.OpCode;
import org.apache.zookeeper.server.EmbeddedServerCnxn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EmbeddedClientCnxnSocket extends ClientCnxnSocket {
	Logger LOG = LoggerFactory.getLogger(EmbeddedClientCnxnSocket.class);

	private EmbeddedServerCnxn embeddedServerCnxn;

	private boolean writeEnabled = true;

	private boolean readEnabled = true;

	public EmbeddedClientCnxnSocket() {
		super();
	}

	@Override
	boolean isConnected() {
		if (embeddedServerCnxn == null) {
			return false;
		}
		return true;
	}

	@Override
	void connect(InetSocketAddress addr) throws IOException {
		embeddedServerCnxn = new EmbeddedServerCnxn(this);
		sendThread.primeConnection();
	}

	@Override
	SocketAddress getRemoteSocketAddress() {
		return new InetSocketAddress("localhost", 0);
	}

	@Override
	SocketAddress getLocalSocketAddress() {
		return new InetSocketAddress("localhost", 0);
	}

	@Override
	void cleanup() {
		close();

	}

	@Override
	void close() {
		embeddedServerCnxn.closeClientSide();
		embeddedServerCnxn = null;
	}

	@Override
	void wakeupCnxn() {
		// noop
	}

	@Override
	void enableWrite() {
		writeEnabled = true;
	}

	@Override
	void disableWrite() {
		writeEnabled = false;
	}

	@Override
	void enableReadWriteOnly() {
		writeEnabled = true;
	}

	@Override
	void doTransport(int waitTimeOut, List<Packet> pendingQueue, LinkedList<Packet> outgoingQueue, ClientCnxn cnxn)
			throws IOException, InterruptedException {
		updateNow();
		if (writeEnabled || readEnabled) {
			doIO(pendingQueue, outgoingQueue, cnxn);
		}
		if (sendThread.getZkState().isConnected()) {
			synchronized (outgoingQueue) {
				if (findSendablePacket(outgoingQueue,
						cnxn.sendThread.clientTunneledAuthenticationInProgress()) != null) {
					enableWrite();
				}
			}
		}
	}

	@Override
	void testableCloseSocket() throws IOException {
		embeddedServerCnxn.closeClientSide();
		embeddedServerCnxn = null;
	}

	@Override
	void sendPacket(Packet p) throws IOException {
		p.createBB();
		ByteBuffer pbb = p.bb;
		embeddedServerCnxn.clientSend(pbb);
	}

	void doIO(List<Packet> pendingQueue, LinkedList<Packet> outgoingQueue, ClientCnxn cnxn)
			throws InterruptedException, IOException {

		// reading
		if (readEnabled) {
			int rc = embeddedServerCnxn.clientRead(incomingBuffer);
			if (rc < 0) {
				throw new EndOfStreamException("Unable to read additional data from server sessionid 0x"
						+ Long.toHexString(sessionId) + ", likely server has closed socket");
			}
			if (!incomingBuffer.hasRemaining()) {
				incomingBuffer.flip();
				if (incomingBuffer == lenBuffer) {
					recvCount++;
					readLength();
				} else if (!initialized) {
					readConnectResult();
					enableRead();
					if (findSendablePacket(outgoingQueue,
							cnxn.sendThread.clientTunneledAuthenticationInProgress()) != null) {
						// Since SASL authentication has completed (if client is
						// configured to do so),
						// outgoing packets waiting in the outgoingQueue can now
						// be sent.
						enableWrite();
					}
					lenBuffer.clear();
					incomingBuffer = lenBuffer;
					updateLastHeard();
					initialized = true;
				} else {
					sendThread.readResponse(incomingBuffer);
					lenBuffer.clear();
					incomingBuffer = lenBuffer;
					updateLastHeard();
				}
			}
		}
		// writing
		if (writeEnabled) {
			synchronized (outgoingQueue) {
				Packet p = findSendablePacket(outgoingQueue, cnxn.sendThread.clientTunneledAuthenticationInProgress());

				if (p != null) {
					updateLastSend();
					// If we already started writing p, p.bb will already exist
					if (p.bb == null) {
						if ((p.requestHeader != null) && (p.requestHeader.getType() != OpCode.ping)
								&& (p.requestHeader.getType() != OpCode.auth)) {
							p.requestHeader.setXid(cnxn.getXid());
						}
						p.createBB();
					}
					embeddedServerCnxn.clientSend(p.bb);
					// sock.write(p.bb);<-- write!
					if (!p.bb.hasRemaining()) {
						sentCount++;
						outgoingQueue.removeFirstOccurrence(p);
						if (p.requestHeader != null && p.requestHeader.getType() != OpCode.ping
								&& p.requestHeader.getType() != OpCode.auth) {
							synchronized (pendingQueue) {
								pendingQueue.add(p);
							}
						}
					}
				}
				if (outgoingQueue.isEmpty()) {
					// No more packets to send: turn off write interest flag.
					// Will be turned on later by a later call to enableWrite(),
					// from within ZooKeeperSaslClient (if client is configured
					// to attempt SASL authentication), or in either doIO() or
					// in doTransport() if not.
					disableWrite();
				} else if (!initialized && p != null && !p.bb.hasRemaining()) {
					// On initial connection, write the complete connect request
					// packet, but then disable further writes until after
					// receiving a successful connection response. If the
					// session is expired, then the server sends the expiration
					// response and immediately closes its end of the socket. If
					// the client is simultaneously writing on its end, then the
					// TCP stack may choose to abort with RST, in which case the
					// client would never receive the session expired event. See
					// http://docs.oracle.com/javase/6/docs/technotes/guides/net/articles/connection_release.html
					disableWrite();
				} else {
					// Just in case
					enableWrite();
				}
			}
		}

	}

	private void enableRead() {
		readEnabled = true;
	}

	private Packet findSendablePacket(LinkedList<Packet> outgoingQueue,
			boolean clientTunneledAuthenticationInProgress) {
		synchronized (outgoingQueue) {
			if (outgoingQueue.isEmpty()) {
				return null;
			}
			if (outgoingQueue.getFirst().bb != null // If we've already starting
													// sending the first packet,
													// we better finish
					|| !clientTunneledAuthenticationInProgress) {
				return outgoingQueue.getFirst();
			}

			// Since client's authentication with server is in progress,
			// send only the null-header packet queued by primeConnection().
			// This packet must be sent so that the SASL authentication process
			// can proceed, but all other packets should wait until
			// SASL authentication completes.
			ListIterator<Packet> iter = outgoingQueue.listIterator();
			while (iter.hasNext()) {
				Packet p = iter.next();
				if (p.requestHeader == null) {
					// We've found the priming-packet. Move it to the beginning
					// of the queue.
					iter.remove();
					outgoingQueue.add(0, p);
					return p;
				} else {
					// Non-priming packet: defer it until later, leaving it in
					// the queue
					// until authentication completes.
					if (LOG.isDebugEnabled()) {
						LOG.debug("deferring non-priming packet: " + p + "until SASL authentication completes.");
					}
				}
			}
			// no sendable packet found.
			return null;
		}
	}
}
