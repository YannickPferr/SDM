package de.tuda.dmdb.sql.operator.exercise;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;

import de.tuda.dmdb.net.TCPClient;
import de.tuda.dmdb.sql.operator.Operator;
import de.tuda.dmdb.sql.operator.SendBase;
import de.tuda.dmdb.storage.AbstractRecord;

/**
 * Implementation of send operator
 * 
 * @author melhindi
 *
 */
public class Send extends SendBase {

	/**
	 * Constructor of Send
	 * 
	 * @param child
	 *            - Child operator used to process next calls, e.g., TableScan or
	 *            Selection
	 * @param nodeId
	 *            - Own nodeId to identify which records to keep locally
	 * @param nodeMap
	 *            - Map containing connection information (as "IP:port" or
	 *            "domain-name:port") to establish connection to other peers
	 * @param partitionColumn
	 *            - Number of column that should be used to repartition the data
	 */
	public Send(Operator child, int nodeId, Map<Integer, String> nodeMap, int partitionColumn) {
		super(child, nodeId, nodeMap, partitionColumn);
	}

	@Override
	public void open() {
		// TODO: implement this method
		// init child
		child.open();
		// create a client socket for all peer nodes using information in nodeMap
		// store client socket in map for later use
		for (Entry<Integer, String> e : nodeMap.entrySet()) {
			//split into host and port
			String[] domain = e.getValue().split(":");
			try {
				connectionMap.put(e.getKey(), new TCPClient(domain[0], Integer.parseInt(domain[1])));
			} catch (NumberFormatException | IOException e1) {
				e1.printStackTrace();
			}
		}
	}

	@Override
	public AbstractRecord next() {
		// TODO: implement this method
		// retrieve next record from child and determine whether to keep record local or
		// send to peer
		AbstractRecord record = null;
		while ((record = child.next()) != null) {
			//Grap nodeId and check if record is supposed to be on this or another node
			int nodeId = getNodeIdForRecord(record, partitionColumn);
			// store locally
			if (nodeId == this.nodeId)
				return record;
			// send to a peer
			else
				connectionMap.get(nodeId).sendRecord(record);
		}
		// when no more child records, close connections to peers
		closeConnectionsToPeers();

		return null;
	}

	@Override
	public void close() {
		// TODO: implement this method
		child.close();
	}

}
