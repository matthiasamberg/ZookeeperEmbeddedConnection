package org.apache.zookeeper.server;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashSet;

import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;

import org.apache.zookeeper.Environment;
import org.apache.zookeeper.server.ServerCnxn;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EmbeddedServerCnxnFactory extends ServerCnxnFactory implements Runnable {
	Logger LOG = LoggerFactory.getLogger(EmbeddedServerCnxnFactory.class);
	private int maxClientCnxns;
	boolean killed = false;
	private InetSocketAddress localAddress;
	private Thread thread;
	private static EmbeddedServerCnxnFactory instance;

	public EmbeddedServerCnxnFactory() {
		instance = this;
	}

	/**
	 * @return null if this factory wasn't instantiated or the EmbeddedServerCnxnFactory instance
	 */
	/*package*/ static EmbeddedServerCnxnFactory getInstance() {
		return instance;
	}

	@Override
	public int getLocalPort() {
		return localAddress.getPort();
	}

	@Override
	public Iterable<ServerCnxn> getConnections() {
		return cnxns;
	}

	@Override
	public void closeSession(long sessionId) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("closeSession sessionid:0x" + sessionId);
		}
		ServerCnxn[] allCnxns = null;
		synchronized (cnxns) {
			allCnxns = cnxns.toArray(new ServerCnxn[cnxns.size()]);
		}
		for (ServerCnxn cnxn : allCnxns) {
			if (cnxn.getSessionId() == sessionId) {
				try {
					cnxn.close();
				} catch (Exception e) {
					LOG.warn("exception during session close", e);
				}
				break;
			}
		}
	}

	@Override
	public void configure(InetSocketAddress addr, int maxClientCnxns) throws IOException {
		configureSaslLogin();
		this.localAddress = addr;
		this.maxClientCnxns = maxClientCnxns;

		thread = new Thread(this, "NIOServerCxn.Factory:" + addr);
		thread.setDaemon(true);
	}

	@Override
	public int getMaxClientCnxnsPerHost() {
		return maxClientCnxns;
	}

	@Override
	public void setMaxClientCnxnsPerHost(int max) {
		maxClientCnxns = max;
	}

	@Override
	public void startup(ZooKeeperServer zks) throws IOException, InterruptedException {
		start();
		zks.startdata();
		zks.startup();
		setZooKeeperServer(zks);
	}
	
	/*package*/ ZooKeeperServer getZkServer() {
		return zkServer;
	}
	@Override
	public void join() throws InterruptedException {
		synchronized (this) {
			while (!killed) {
				wait();
			}
		}

	}

	@Override
	public void shutdown() {
		LOG.info("shutdown called " + localAddress);
		if (login != null) {
			login.shutdown();
		}

		closeAll();
		synchronized (this) {
			killed = true;
			notifyAll();
		}

	}

	@Override
	public void start() {
		if (thread.getState() == Thread.State.NEW) {
			thread.start();
		}
	}

	@Override
	public void closeAll() {
		if (LOG.isDebugEnabled()) {
			LOG.debug("closeAll()");
		}

		ServerCnxn[] allCnxns = null;
		synchronized (cnxns) {
			allCnxns = cnxns.toArray(new ServerCnxn[cnxns.size()]);
		}
		// got to clear all the connections that we have in the selector
		for (ServerCnxn cnxn : allCnxns) {
			try {
				cnxn.close();
			} catch (Exception e) {
				LOG.warn("Ignoring exception closing cnxn sessionid 0x" + Long.toHexString(cnxn.getSessionId()), e);
			}
		}
		if (LOG.isDebugEnabled()) {
			LOG.debug("cnxns size:" + allCnxns.length);
		}

	}

	@Override
	public InetSocketAddress getLocalAddress() {
		return localAddress;
	}

	public void addCnxn(EmbeddedServerCnxn cnxn) {
		synchronized (cnxns) {
			cnxns.add(cnxn);
		}
	}

	@Override
	public void run() {
		while (true) {
			Iterable<ServerCnxn> connections = getConnections();
			for (ServerCnxn serverCnxn : connections) {
				try {
					((EmbeddedServerCnxn)serverCnxn).doIO();
				} catch (Exception e) {
					  LOG.warn("Ignoring unexpected exception", e);
				}
			}
			try {
				Thread.sleep(25);
			} catch (InterruptedException e) {
			}
		}
	}

	// fixes from NIOServerCnxnFactoryUnblu regarding SASL auth issues with JBOSS
	/**
	 * kh (unblu): customized according to fix on client side. See
	 * org.apache.zookeeper.client.ZooKeeperSaslClient.ZooKeeperSaslClient(
	 * String) and Zookeeper issue
	 * https://issues.apache.org/jira/browse/ZOOKEEPER-1696
	 * 
	 * 
	 * Initialize the server SASL if specified.
	 *
	 * If the user has specified a "ZooKeeperServer.LOGIN_CONTEXT_NAME_KEY" or a
	 * jaas.conf using "java.security.auth.login.config" the authentication is
	 * required and an exception is raised. Otherwise no authentication is
	 * configured and no exception is raised.
	 *
	 * @throws IOException
	 *             if jaas.conf is missing or there's an error in it.
	 */
	@Override
	protected void configureSaslLogin() throws IOException {
		String serverSection = System.getProperty(ZooKeeperSaslServer.LOGIN_CONTEXT_NAME_KEY,
				ZooKeeperSaslServer.DEFAULT_LOGIN_CONTEXT_NAME);

		// Note that 'Configuration' here refers to
		// javax.security.auth.login.Configuration.
		AppConfigurationEntry entries[] = null;
		RuntimeException runtimeException = null;
		try {
			entries = Configuration.getConfiguration().getAppConfigurationEntry(serverSection);
		} catch (SecurityException e) {
			// handle below: might be harmless if the user doesn't intend to use
			// JAAS authentication.
			runtimeException = e;
		} catch (IllegalArgumentException e) {
			// third party customized getAppConfigurationEntry could throw
			// IllegalArgumentException when JAAS
			// configuration isn't set. We can reevaluate whether to catch
			// RuntimeException instead when more
			// different types of RuntimeException found
			runtimeException = e;
		}

		// No entries in jaas.conf
		// If there's a configuration exception fetching the jaas section and
		// the user has required sasl by specifying a LOGIN_CONTEXT_NAME_KEY or
		// a jaas file
		// we throw an exception otherwise we continue without authentication.
		if (entries == null) {
			String jaasFile = System.getProperty(Environment.JAAS_CONF_KEY);
			String loginContextName = System.getProperty(ZooKeeperSaslServer.LOGIN_CONTEXT_NAME_KEY);
			if (runtimeException != null && (loginContextName != null || jaasFile != null)) {
				String errorMessage = "No JAAS configuration section named '" + serverSection + "' was found";
				if (jaasFile != null) {
					errorMessage += "in '" + jaasFile + "'.";
				}
				if (loginContextName != null) {
					errorMessage += " But " + ZooKeeperSaslServer.LOGIN_CONTEXT_NAME_KEY + " was set.";
				}
				LOG.error(errorMessage);
				throw new IOException(errorMessage);
			}
			return;
		}

		return;

		// old implementation, did cause problems in jboss... we never use sasl,
		// so just ignore it
		// // jaas.conf entry available
		// try {
		// saslServerCallbackHandler = new
		// SaslServerCallbackHandler(Configuration.getConfiguration());
		// login = new Login(serverSection, saslServerCallbackHandler);
		// login.startThreadIfNeeded();
		// } catch (LoginException e) {
		// throw new IOException("Could not configure server because SASL
		// configuration did not allow the "
		// + " ZooKeeper server to authenticate itself properly: " + e);
		// }
	}

}
