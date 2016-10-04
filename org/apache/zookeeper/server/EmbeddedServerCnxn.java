package org.apache.zookeeper.server;

import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.io.PrintWriter;
import java.io.Writer;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.SelectionKey;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.jute.BinaryInputArchive;
import org.apache.jute.BinaryOutputArchive;
import org.apache.jute.Record;
import org.apache.zookeeper.Environment;
import org.apache.zookeeper.EmbeddedClientCnxnSocket;
import org.apache.zookeeper.Version;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.proto.ReplyHeader;
import org.apache.zookeeper.proto.RequestHeader;
import org.apache.zookeeper.proto.WatcherEvent;
import org.apache.zookeeper.server.quorum.Leader;
import org.apache.zookeeper.server.quorum.LeaderZooKeeperServer;
import org.apache.zookeeper.server.quorum.ReadOnlyZooKeeperServer;
import org.apache.zookeeper.server.util.OSMXBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EmbeddedServerCnxn extends ServerCnxn {
	Logger LOG = LoggerFactory.getLogger(EmbeddedServerCnxn.class);

	private volatile ZooKeeperServer zkServer;

	private int sessionTimeout;
	private long sessionId;

	private boolean receive = true;

	ByteBuffer lenBuffer = ByteBuffer.allocate(4);

	ByteBuffer incomingBuffer = lenBuffer;

	EmbeddedServerCnxnFactory factory;
	private boolean initialized;

	private EmbeddedClientCnxnSocket clientCnxn;
	private PipedInputStream serverInputStream;
	private PipedOutputStream serverOutputStream;
	private PipedInputStream clientInputStream;
	private PipedOutputStream clientOutputStream;

	private EmbeddedServerCnxnFactory embeddedServerCnxnFactory;

	private boolean closedClientSide = false;

	private boolean closedServerSide = false;
	/**
	 * The number of requests that have been submitted but not yet responded to.
	 */
	int outstandingRequests;

	private int outstandingLimit = 1;

	public EmbeddedServerCnxn(EmbeddedClientCnxnSocket clientCnxn) throws IOException {
		serverInputStream = new PipedInputStream();
		serverOutputStream = new PipedOutputStream(serverInputStream);
		clientInputStream = new PipedInputStream();
		clientOutputStream = new PipedOutputStream(clientInputStream);
		this.clientCnxn = clientCnxn;
		embeddedServerCnxnFactory = EmbeddedServerCnxnFactory.getInstance();
		if (embeddedServerCnxnFactory == null) {
			throw new IOException("no embeddedServerCnxnFactory");
		}
		embeddedServerCnxnFactory.addCnxn(this);
		factory = embeddedServerCnxnFactory;

		this.zkServer = embeddedServerCnxnFactory.getZkServer();
		if (zkServer != null) {
			outstandingLimit = zkServer.getGlobalOutstandingLimit();
		}
	}

	@Override
	int getSessionTimeout() {
		return sessionTimeout;
	}

	@Override
	void close() {
		closedServerSide = true;
	}

	private static final byte[] fourBytes = new byte[4];

	@Override
	public void sendResponse(ReplyHeader h, Record r, String tag) throws IOException {
		if (closedServerSide) {
			return;
		}
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		// Make space for length
		BinaryOutputArchive bos = BinaryOutputArchive.getArchive(baos);
		try {
			baos.write(fourBytes);
			bos.writeRecord(h, "header");
			if (r != null) {
				bos.writeRecord(r, tag);
			}
			baos.close();
		} catch (IOException e) {
			LOG.error("Error serializing response");
		}
		byte b[] = baos.toByteArray();
		ByteBuffer bb = ByteBuffer.wrap(b);
		bb.putInt(b.length - 4).rewind();
		sendBuffer(bb);
		enableRecv();
		if (h.getXid() > 0) {
			synchronized (this) {
				outstandingRequests--;
			}
			// check throttling
			synchronized (this.factory) {
				if (zkServer.getInProcess() < outstandingLimit || outstandingRequests < 1) {
					enableRecv();
				}
			}
		}

	}

	@Override
	void sendCloseSession() {
        sendBuffer(ServerCnxnFactory.closeConn);
	}

	protected void incrOutstandingRequests(RequestHeader h) {
		if (h.getXid() >= 0) {
			synchronized (this) {
				outstandingRequests++;
			}
			synchronized (this.factory) {
				// check throttling
				if (zkServer.getInProcess() > outstandingLimit) {
					if (LOG.isDebugEnabled()) {
						LOG.debug("Throttling recv " + zkServer.getInProcess());
					}
					disableRecv();
				}
			}
		}

	}

	@Override
	public void process(WatchedEvent event) {
		ReplyHeader h = new ReplyHeader(-1, -1L, 0);
		if (LOG.isTraceEnabled()) {
			ZooTrace.logTraceMessage(LOG, ZooTrace.EVENT_DELIVERY_TRACE_MASK,
					"Deliver event " + event + " to 0x" + Long.toHexString(this.sessionId) + " through " + this);
		}

		// Convert WatchedEvent to a type that can be sent over the wire
		WatcherEvent e = event.getWrapper();
		try {
			sendResponse(h, e, "notification");
		} catch (IOException e1) {
			if (LOG.isDebugEnabled()) {
				LOG.debug("Problem sending to " + getRemoteSocketAddress(), e1);
			}
			close();
		}
		;
	}

	@Override
	long getSessionId() {
		return sessionId;
	}

	@Override
	void setSessionId(long sessionId) {
		this.sessionId = sessionId;

	}

	@Override
	void sendBuffer(ByteBuffer closeConn) {
		try {
			serverSend(closeConn);
		} catch (IOException e) {
			LOG.error("Unexpected Exception: ", e);
		}
	}

	@Override
	void enableRecv() {
		receive = true;

	}

	@Override
	void disableRecv() {
		receive = false;
	}

	@Override
	void setSessionTimeout(int sessionTimeout) {
		this.sessionTimeout = sessionTimeout;

	}

	@Override
	protected ServerStats serverStats() {
		if (zkServer == null) {
			return null;
		}
		return zkServer.serverStats();
	}

	@Override
	public long getOutstandingRequests() {
        synchronized (this) {
            synchronized (this.factory) {
                return outstandingRequests;
            }
        }
	}

	@Override
	public InetSocketAddress getRemoteSocketAddress() {
		InetAddress addr;
		try {
			addr = InetAddress.getByName("localhost");
			return new InetSocketAddress(addr, 0);
		} catch (UnknownHostException e) {
			LOG.warn("no InetAddress found for 'localhost'", e);
		}
		return null;
	}

    @Override
    public int getInterestOps() {
    	int ops=0;
    	if (!closedClientSide && !closedServerSide) {
    		ops = ops | SelectionKey.OP_READ;
    		if (receive) {
    			ops = ops | SelectionKey.OP_WRITE;
    		}
    	}
    	
        return ops;
    }


	public void doIO() throws InterruptedException {
		try {
			if (receive) {
				int rc = serverRead(incomingBuffer);
				if (rc < 0) {
					throw new EndOfStreamException("Unable to read additional data from client sessionid 0x"
							+ Long.toHexString(sessionId) + ", likely client has closed socket");
				}
				if (incomingBuffer.remaining() == 0) {
					boolean isPayload;
					if (incomingBuffer == lenBuffer) { // start of next request
						incomingBuffer.flip();
						isPayload = readLength();
						incomingBuffer.clear();
					} else {
						// continuation
						isPayload = true;
					}
					if (isPayload) { // not the case for 4letterword
						readPayload();
					} else {
						// four letter words take care
						// need not do anything else
						return;
					}
				}
			}
		} catch (EndOfStreamException e) {
			LOG.warn("caught end of stream exception", e); // tell user why

			// expecting close to log session closure
			close();
		} catch (IOException e) {
			LOG.warn("Exception causing close of session 0x" + Long.toHexString(sessionId) + " due to " + e, e);
			if (LOG.isDebugEnabled()) {
				LOG.debug("IOException stack trace", e);
			}
			close();
		}
	}

	/**
	 * Reads the first 4 bytes of lenBuffer, which could be true length or four
	 * letter word.
	 *
	 * @return true if length read, otw false (wasn't really the length)
	 * @throws IOException
	 *             if buffer size exceeds maxBuffer size
	 */
	private boolean readLength() throws IOException {
		// Read the length, now get the buffer
		int len = lenBuffer.getInt();
		if (!initialized && checkFourLetterWord(len)) {
			return false;
		}
		if (len < 0 || len > BinaryInputArchive.maxBuffer) {
			throw new IOException("Len error " + len);
		}
		if (zkServer == null) {
			throw new IOException("ZooKeeperServer not running");
		}
		incomingBuffer = ByteBuffer.allocate(len);
		return true;
	}

	/** Return if four letter word found and responded to, otw false **/
	private boolean checkFourLetterWord(final int len) throws IOException {
		// We take advantage of the limited size of the length to look
		// for cmds. They are all 4-bytes which fits inside of an int
		String cmd = cmd2String.get(len);
		if (cmd == null) {
			return false;
		}
		LOG.info("Processing " + cmd + " command from localservercnxn");
		packetReceived();

		/**
		 * cancel the selection key to remove the socket handling from selector.
		 * This is to prevent netcat problem wherein netcat immediately closes
		 * the sending side after sending the commands and still keeps the
		 * receiving channel open. The idea is to remove the selectionkey from
		 * the selector so that the selector does not notice the closed read on
		 * the socket channel and keep the socket alive to write the data to and
		 * makes sure to close the socket after its done writing the data
		 */

		final PrintWriter pwriter = new PrintWriter(new BufferedWriter(new SendBufferWriter()));
		if (len == ruokCmd) {
			RuokCommand ruok = new RuokCommand(pwriter);
			ruok.start();
			return true;
		} else if (len == getTraceMaskCmd) {
			TraceMaskCommand tmask = new TraceMaskCommand(pwriter);
			tmask.start();
			return true;
		} else if (len == setTraceMaskCmd) {
			int rc = serverRead(incomingBuffer);
			if (rc < 0) {
				throw new IOException("Read error");
			}

			incomingBuffer.flip();
			long traceMask = incomingBuffer.getLong();
			ZooTrace.setTextTraceLevel(traceMask);
			SetTraceMaskCommand setMask = new SetTraceMaskCommand(pwriter, traceMask);
			setMask.start();
			return true;
		} else if (len == enviCmd) {
			EnvCommand env = new EnvCommand(pwriter);
			env.start();
			return true;
		} else if (len == confCmd) {
			ConfCommand ccmd = new ConfCommand(pwriter);
			ccmd.start();
			return true;
		} else if (len == srstCmd) {
			StatResetCommand strst = new StatResetCommand(pwriter);
			strst.start();
			return true;
		} else if (len == crstCmd) {
			CnxnStatResetCommand crst = new CnxnStatResetCommand(pwriter);
			crst.start();
			return true;
		} else if (len == dumpCmd) {
			DumpCommand dump = new DumpCommand(pwriter);
			dump.start();
			return true;
		} else if (len == statCmd || len == srvrCmd) {
			StatCommand stat = new StatCommand(pwriter, len);
			stat.start();
			return true;
		} else if (len == consCmd) {
			ConsCommand cons = new ConsCommand(pwriter);
			cons.start();
			return true;
		} else if (len == wchpCmd || len == wchcCmd || len == wchsCmd) {
			WatchCommand wcmd = new WatchCommand(pwriter, len);
			wcmd.start();
			return true;
		} else if (len == mntrCmd) {
			MonitorCommand mntr = new MonitorCommand(pwriter);
			mntr.start();
			return true;
		} else if (len == isroCmd) {
			IsroCommand isro = new IsroCommand(pwriter);
			isro.start();
			return true;
		}
		return false;
	}
	/// Local Connection Stuff

	public void serverSend(ByteBuffer b) throws IOException {
		if (closedClientSide) {
			throw new IOException("Connection closed!");
		}
		byte[] arr = buffer2array(b);
		clientOutputStream.write(arr);
	}

	public int serverRead(ByteBuffer b) {
		if (closedServerSide) {
			return -1;
		}
		try {
			int numBytes = Math.min(b.remaining(), serverInputStream.available());
			byte[] arr = new byte[numBytes];
			int bytesRead=serverInputStream.read(arr);
			b.put(arr,0,bytesRead);
			return bytesRead;
		} catch (IOException e) {
			return -1;
		}
	}

	public void clientSend(ByteBuffer b) throws IOException {
		if (closedServerSide) {
			throw new IOException("Connection closed!");
		}
		byte[] arr = buffer2array(b);
		serverOutputStream.write(arr);
	}

	public int clientRead(ByteBuffer b) {
		if (closedClientSide) {
			return -1;
		}

		try {
			int numBytes = Math.min(b.remaining(), clientInputStream.available());
			byte[] arr = new byte[numBytes];
			int bytesRead=clientInputStream.read(arr);
			b.put(arr,0,bytesRead);
			return bytesRead;
		} catch (IOException e) {
			return -1;
		}
	}

	private byte[] buffer2array(ByteBuffer b) {
		b.rewind();
		int size = b.remaining();
		byte[] arr = new byte[size];
		b.get(arr);
		return arr;
	}

	public void closeClientSide() {
		closedClientSide = true;
		embeddedServerCnxnFactory = null;
	}

	/**
	 * This class wraps the sendBuffer method of NIOServerCnxn. It is
	 * responsible for chunking up the response to a client. Rather than
	 * cons'ing up a response fully in memory, which may be large for some
	 * commands, this class chunks up the result.
	 */
	private class SendBufferWriter extends Writer {
		private StringBuffer sb = new StringBuffer();

		/**
		 * Check if we are ready to send another chunk.
		 * 
		 * @param force
		 *            force sending, even if not a full chunk
		 */
		private void checkFlush(boolean force) {
			if ((force && sb.length() > 0) || sb.length() > 2048) {
				sendBufferSync(ByteBuffer.wrap(sb.toString().getBytes()));
				// clear our internal buffer
				sb.setLength(0);
			}
		}

		/**
		 * send buffer without using the asynchronous calls to selector and then
		 * close the socket
		 * 
		 * @param bb
		 */
		void sendBufferSync(ByteBuffer bb) {
			try {
				/*
				 * configure socket to be blocking so that we dont have to do
				 * write in a tight while loop
				 */
				// sock.configureBlocking(true);
				if (bb != ServerCnxnFactory.closeConn) {
					/*
					 * if (sock.isOpen()) { sock.write(bb); }
					 */
					serverSend(bb);
					packetSent();
				}
			} catch (IOException ie) {
				LOG.error("Error sending data synchronously ", ie);
			}
		}

		@Override
		public void close() throws IOException {
			if (sb == null)
				return;
			checkFlush(true);
			sb = null; // clear out the ref to ensure no reuse
		}

		@Override
		public void flush() throws IOException {
			checkFlush(true);
		}

		@Override
		public void write(char[] cbuf, int off, int len) throws IOException {
			sb.append(cbuf, off, len);
			checkFlush(false);
		}
	}

	private static final String ZK_NOT_SERVING = "This ZooKeeper instance is not currently serving requests";

	/**
	 * Set of threads for commmand ports. All the 4 letter commands are run via
	 * a thread. Each class maps to a corresponding 4 letter command.
	 * CommandThread is the abstract class from which all the others inherit.
	 */
	private abstract class CommandThread extends Thread {
		PrintWriter pw;

		CommandThread(PrintWriter pw) {
			this.pw = pw;
		}

		public void run() {
			try {
				commandRun();
			} catch (IOException ie) {
				LOG.error("Error in running command ", ie);
			} finally {
				cleanupWriterSocket(pw);
			}
		}

		public abstract void commandRun() throws IOException;
	}

	private class RuokCommand extends CommandThread {
		public RuokCommand(PrintWriter pw) {
			super(pw);
		}

		@Override
		public void commandRun() {
			pw.print("imok");

		}
	}

	private class TraceMaskCommand extends CommandThread {
		TraceMaskCommand(PrintWriter pw) {
			super(pw);
		}

		@Override
		public void commandRun() {
			long traceMask = ZooTrace.getTextTraceLevel();
			pw.print(traceMask);
		}
	}

	private class SetTraceMaskCommand extends CommandThread {
		long trace = 0;

		SetTraceMaskCommand(PrintWriter pw, long trace) {
			super(pw);
			this.trace = trace;
		}

		@Override
		public void commandRun() {
			pw.print(trace);
		}
	}

	private class EnvCommand extends CommandThread {
		EnvCommand(PrintWriter pw) {
			super(pw);
		}

		@Override
		public void commandRun() {
			List<Environment.Entry> env = Environment.list();

			pw.println("Environment:");
			for (Environment.Entry e : env) {
				pw.print(e.getKey());
				pw.print("=");
				pw.println(e.getValue());
			}

		}
	}

	private class ConfCommand extends CommandThread {
		ConfCommand(PrintWriter pw) {
			super(pw);
		}

		@Override
		public void commandRun() {
			if (zkServer == null) {
				pw.println(ZK_NOT_SERVING);
			} else {
				zkServer.dumpConf(pw);
			}
		}
	}

	private class StatResetCommand extends CommandThread {
		public StatResetCommand(PrintWriter pw) {
			super(pw);
		}

		@Override
		public void commandRun() {
			if (zkServer == null) {
				pw.println(ZK_NOT_SERVING);
			} else {
				zkServer.serverStats().reset();
				pw.println("Server stats reset.");
			}
		}
	}

	private class CnxnStatResetCommand extends CommandThread {
		public CnxnStatResetCommand(PrintWriter pw) {
			super(pw);
		}

		@Override
		public void commandRun() {
			if (zkServer == null) {
				pw.println(ZK_NOT_SERVING);
			} else {
				synchronized (factory.cnxns) {
					for (ServerCnxn c : factory.cnxns) {
						c.resetStats();
					}
				}
				pw.println("Connection stats reset.");
			}
		}
	}

	private class DumpCommand extends CommandThread {
		public DumpCommand(PrintWriter pw) {
			super(pw);
		}

		@Override
		public void commandRun() {
			if (zkServer == null) {
				pw.println(ZK_NOT_SERVING);
			} else {
				pw.println("SessionTracker dump:");
				zkServer.sessionTracker.dumpSessions(pw);
				pw.println("ephemeral nodes dump:");
				zkServer.dumpEphemerals(pw);
			}
		}
	}

	private class StatCommand extends CommandThread {
		int len;

		public StatCommand(PrintWriter pw, int len) {
			super(pw);
			this.len = len;
		}

		@SuppressWarnings("unchecked")
		@Override
		public void commandRun() {
			if (zkServer == null) {
				pw.println(ZK_NOT_SERVING);
			} else {
				pw.print("Zookeeper version: ");
				pw.println(Version.getFullVersion());
				if (zkServer instanceof ReadOnlyZooKeeperServer) {
					pw.println("READ-ONLY mode; serving only " + "read-only clients");
				}
				if (len == statCmd) {
					LOG.info("Stat command output");
					pw.println("Clients:");
					// clone should be faster than iteration
					// ie give up the cnxns lock faster
					HashSet<NIOServerCnxn> cnxnset;
					synchronized (factory.cnxns) {
						cnxnset = (HashSet<NIOServerCnxn>) factory.cnxns.clone();
					}
					for (NIOServerCnxn c : cnxnset) {
						c.dumpConnectionInfo(pw, true);
						pw.println();
					}
					pw.println();
				}
				pw.print(zkServer.serverStats().toString());
				pw.print("Node count: ");
				pw.println(zkServer.getZKDatabase().getNodeCount());
			}

		}
	}

	private class ConsCommand extends CommandThread {
		public ConsCommand(PrintWriter pw) {
			super(pw);
		}

		@SuppressWarnings("unchecked")
		@Override
		public void commandRun() {
			if (zkServer == null) {
				pw.println(ZK_NOT_SERVING);
			} else {
				// clone should be faster than iteration
				// ie give up the cnxns lock faster
				HashSet<NIOServerCnxn> cnxns;
				synchronized (factory.cnxns) {
					cnxns = (HashSet<NIOServerCnxn>) factory.cnxns.clone();
				}
				for (NIOServerCnxn c : cnxns) {
					c.dumpConnectionInfo(pw, false);
					pw.println();
				}
				pw.println();
			}
		}
	}

	private class WatchCommand extends CommandThread {
		int len = 0;

		public WatchCommand(PrintWriter pw, int len) {
			super(pw);
			this.len = len;
		}

		@Override
		public void commandRun() {
			if (zkServer == null) {
				pw.println(ZK_NOT_SERVING);
			} else {
				DataTree dt = zkServer.getZKDatabase().getDataTree();
				if (len == wchsCmd) {
					dt.dumpWatchesSummary(pw);
				} else if (len == wchpCmd) {
					dt.dumpWatches(pw, true);
				} else {
					dt.dumpWatches(pw, false);
				}
				pw.println();
			}
		}
	}

	private class MonitorCommand extends CommandThread {

		MonitorCommand(PrintWriter pw) {
			super(pw);
		}

		@Override
		public void commandRun() {
			if (zkServer == null) {
				pw.println(ZK_NOT_SERVING);
				return;
			}
			ZKDatabase zkdb = zkServer.getZKDatabase();
			ServerStats stats = zkServer.serverStats();

			print("version", Version.getFullVersion());

			print("avg_latency", stats.getAvgLatency());
			print("max_latency", stats.getMaxLatency());
			print("min_latency", stats.getMinLatency());

			print("packets_received", stats.getPacketsReceived());
			print("packets_sent", stats.getPacketsSent());
			print("num_alive_connections", stats.getNumAliveClientConnections());

			print("outstanding_requests", stats.getOutstandingRequests());

			print("server_state", stats.getServerState());
			print("znode_count", zkdb.getNodeCount());

			print("watch_count", zkdb.getDataTree().getWatchCount());
			print("ephemerals_count", zkdb.getDataTree().getEphemeralsCount());
			print("approximate_data_size", zkdb.getDataTree().approximateDataSize());

			OSMXBean osMbean = new OSMXBean();
			if (osMbean != null && osMbean.getUnix() == true) {
				print("open_file_descriptor_count", osMbean.getOpenFileDescriptorCount());
				print("max_file_descriptor_count", osMbean.getMaxFileDescriptorCount());
			}

			if (stats.getServerState().equals("leader")) {
				Leader leader = ((LeaderZooKeeperServer) zkServer).getLeader();

				print("followers", leader.getLearners().size());
				print("synced_followers", leader.getForwardingFollowers().size());
				print("pending_syncs", leader.getNumPendingSyncs());
			}
		}

		private void print(String key, long number) {
			print(key, "" + number);
		}

		private void print(String key, String value) {
			pw.print("zk_");
			pw.print(key);
			pw.print("\t");
			pw.println(value);
		}

	}

	private class IsroCommand extends CommandThread {

		public IsroCommand(PrintWriter pw) {
			super(pw);
		}

		@Override
		public void commandRun() {
			if (zkServer == null) {
				pw.print("null");
			} else if (zkServer instanceof ReadOnlyZooKeeperServer) {
				pw.print("ro");
			} else {
				pw.print("rw");
			}
		}
	}

	/**
	 * clean up the socket related to a command and also make sure we flush the
	 * data before we do that
	 * 
	 * @param pwriter
	 *            the pwriter for a command socket
	 */
	private void cleanupWriterSocket(PrintWriter pwriter) {
		try {
			if (pwriter != null) {
				pwriter.flush();
				pwriter.close();
			}
		} catch (Exception e) {
			LOG.info("Error closing PrintWriter ", e);
		} finally {
			try {
				close();
			} catch (Exception e) {
				LOG.error("Error closing a command socket ", e);
			}
		}
	}

	/** Read the request payload (everything following the length prefix) */
	private void readPayload() throws IOException, InterruptedException {
		if (incomingBuffer.remaining() != 0) { // have we read length bytes?
			int rc = serverRead(incomingBuffer); // sock is non-blocking, so ok
			if (rc < 0) {
				throw new EndOfStreamException("Unable to read additional data from client sessionid 0x"
						+ Long.toHexString(sessionId) + ", likely client has closed socket");
			}
		}

		if (incomingBuffer.remaining() == 0) { // have we read length bytes?
			packetReceived();
			incomingBuffer.flip();
			if (!initialized) {
				readConnectRequest();
			} else {
				readRequest();
			}
			lenBuffer.clear();
			incomingBuffer = lenBuffer;
		}
	}

	private void readConnectRequest() throws IOException, InterruptedException {
		if (zkServer == null) {
			throw new IOException("ZooKeeperServer not running");
		}
		zkServer.processConnectRequest(this, incomingBuffer);
		initialized = true;
	}

	private void readRequest() throws IOException {
		zkServer.processPacket(this, incomingBuffer);
	}

	public int getServerSendBufferSize() {
		return 10;
	}

}
