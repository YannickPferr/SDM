package de.tuda.dmdb.sql.operator.exercise;

import java.io.IOException;
import de.tuda.dmdb.net.TCPServer;
import de.tuda.dmdb.sql.operator.Operator;
import de.tuda.dmdb.sql.operator.ReceiveBase;
import de.tuda.dmdb.storage.AbstractRecord;

import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Implementation of receive operator
 * 
 * @author melhindi
 *
 */
public class Receive extends ReceiveBase {

	/**
	 * Constructor of Receive
	 * 
	 * @param child
	 *            - Child operator used to process next calls, usually SendOperator
	 * @param numPeers
	 *            - Number of peer nodes that have to finish processing before
	 *            operator finishes
	 * @param listenerPort
	 *            - Port on which to bind receive server
	 * @param nodeId
	 *            - Own nodeId, used for debugging
	 */
	public Receive(Operator child, int numPeers, int listenerPort, int nodeId) {
		super(child, numPeers, listenerPort, nodeId);
	}

	@Override
	public void open() {
		// TODO: implement this method
		// HINT: local cache must be passed to TCPServer
		// and will be accessed by multiple Handler-Threads - take multi-threading into
		// account where applicable!
		// init local cache
		localCache = new ConcurrentLinkedQueue<>();
		try {
			// init and start server
			receiveServer = new TCPServer(listenerPort, localCache, finishedPeers);
			receiveServer.start();

			// Attention: call open on child after starting receive server, so that
			// sendOperator can connect.
			child.open();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public AbstractRecord next() {
		// TODO: implement this method
		// HINT: local cache must be passed to TCPServer
		// and will be accessed by multiple Handler-Threads - take multi-threading into
		// account where applicable!

		// process local records
		AbstractRecord record = null;
		while ((record = child.next()) != null)
			return record;

		// process received records
		while ((record = localCache.poll()) != null)
			return record;

		// check if we finished processing of all records - hint: you can use
		// this.finishedPeers
		// if not all peers are finished yet, wait for them to finish and then poll
		// again
		while (finishedPeers.get() <= numPeers)
			;
		// process received records again to poll the last remaining
		while ((record = localCache.poll()) != null)
			return record;

		return null;
	}

	@Override
	public void close() {
		// TODO: implement this method
		// reverse what was done in open()
		receiveServer.stopServer();
		child.close();
	}

}
