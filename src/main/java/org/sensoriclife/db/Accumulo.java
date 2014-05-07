package org.sensoriclife.db;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;

/**
 * 
 * @author jnphilipp
 * @version 0.0.2
 */
public class Accumulo {
	/**
	 * instance
	 */
	private static Accumulo accumolo;
	private Instance instance; 
	private Connector connector;

	private Accumulo() {
		this.instance = null;
		this.connector = null;
	}

	public static synchronized Accumulo getInstance() {
		if ( accumolo == null )
			accumolo = new Accumulo();

		return accumolo;
	}

	public void connect() {
		this.instance = new MockInstance();
	}

	public void connect(String name, String zooServers, String user, String password) throws AccumuloException, AccumuloSecurityException {
		this.instance = new ZooKeeperInstance(name, zooServers);
		this.connector = this.instance.getConnector(user, new PasswordToken(password));
	}

	public void disconnect() {
		this.instance = null;
		this.connector = null;
	}

	public synchronized Scanner scannByKey(String tabel, String key, String attribute, String authentication) throws TableNotFoundException {
		Authorizations auths = new Authorizations(authentication);

		Scanner scan = this.connector.createScanner(tabel, auths);
		scan.setRange(new Range(key,key));
		scan.fetchColumnFamily(new Text(attribute));

		return scan;
	}

	public synchronized void write(String table, Text rowID, Text columnFamily, Text columnQualifier, Value values) throws TableNotFoundException, MutationsRejectedException {
		this.write(table, rowID, columnFamily, columnQualifier, System.currentTimeMillis(), values, "public");
	}

	public synchronized void write(String table, Text rowID, Text columnFamily, Text columnQualifier, Value values, String visibility) throws TableNotFoundException, MutationsRejectedException {
		this.write(table, rowID, columnFamily, columnQualifier, System.currentTimeMillis(), values, visibility);
	}

	public synchronized void write(String table, Text rowID, Text columnFamily, Text columnQualifier, long timestamp, Value values) throws TableNotFoundException, MutationsRejectedException {
		this.write(table, rowID, columnFamily, columnQualifier, timestamp, values, "public");
	}

	public synchronized void write(String table, Text rowID, Text columnFamily, Text columnQualifier, long timestamp, Value value, String visibility) throws TableNotFoundException, MutationsRejectedException {
		ColumnVisibility colVis = new ColumnVisibility("public");

		Mutation mutation = new Mutation(rowID);
		mutation.put(columnFamily, columnQualifier, colVis, timestamp, value);

		if ( this.connector != null ) {
			BatchWriterConfig config = new BatchWriterConfig();
			config.setMaxMemory(10000000L);
			BatchWriter writer = this.connector.createBatchWriter(table, config);
			writer.addMutation(mutation);
			writer.close();
		}
	}
}