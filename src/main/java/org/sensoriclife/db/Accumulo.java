package org.sensoriclife.db;

import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;

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

	public void connect() throws AccumuloException, AccumuloSecurityException {
		this.instance = new MockInstance();
		this.connector = this.instance.getConnector("",  new PasswordToken(""));
	}

	public void connect(String name, String zooServers, String user, String password) throws AccumuloException, AccumuloSecurityException {
		this.instance = new ZooKeeperInstance(name, zooServers);
		this.connector = this.instance.getConnector(user, new PasswordToken(password));
	}

	public void disconnect() {
		this.instance = null;
		this.connector = null;
	}

	public void createTable(String table) throws AccumuloException, AccumuloSecurityException, TableExistsException {
		this.connector.tableOperations().create(table);
	}

	public synchronized Iterator<Entry<Key,Value>> scannByKey(String table, Range range) throws TableNotFoundException {
		return this.scannByKey(table, "public", range);
	}

	public synchronized Iterator<Entry<Key,Value>> scannByKey(String table, String visibility, Range range) throws TableNotFoundException {
		Authorizations auths = new Authorizations(visibility);

		Scanner scan = this.connector.createScanner(table, auths);
		scan.setRange(range);

		return scan.iterator();
	}

	public synchronized void write(String table, String rowId, String columnFamily, String columnQualifier, Value values) throws MutationsRejectedException, TableNotFoundException {
		this.write(table, rowId, columnFamily, columnQualifier, "public", System.currentTimeMillis(), values);
	}

	public synchronized void write(String table, String rowId, String columnFamily, String columnQualifier, Value values, String visibility) throws MutationsRejectedException, TableNotFoundException {
		this.write(table, rowId, columnFamily, columnQualifier, visibility, System.currentTimeMillis(), values);
	}

	public synchronized void write(String table, String rowId, String columnFamily, String columnQualifier, long timestamp, Value values) throws MutationsRejectedException, TableNotFoundException {
		this.write(table, rowId, columnFamily, columnQualifier, "public", timestamp, values);
	}

	public synchronized void write(String table, String rowId, String columnFamily, String columnQualifier, String visibility, long timestamp, Value value) throws MutationsRejectedException, TableNotFoundException {
		ColumnVisibility colVis = new ColumnVisibility("public");

		BatchWriterConfig config = new BatchWriterConfig();
		config.setMaxMemory(10000000L);
		BatchWriter writer = this.connector.createBatchWriter(table, config);

		Mutation mutation = new Mutation(rowId);
		mutation.put(columnFamily, columnQualifier, colVis, timestamp, value);
		writer.addMutation(mutation);

		writer.close();
	}

	public synchronized void write(String table, List<Object[]> rows) throws MutationsRejectedException, TableNotFoundException {
		BatchWriterConfig config = new BatchWriterConfig();
		config.setMaxMemory(10000000L);
		BatchWriter writer = this.connector.createBatchWriter(table, config);

		for ( Object[] row : rows ) {
			Mutation mutation = new Mutation(row[0].toString());
			mutation.put(row[1].toString(), row[2].toString(), new ColumnVisibility(row[3].toString()), (long)row[4], (Value)row[5]);
			writer.addMutation(mutation);
		}

		writer.close();
	}
}