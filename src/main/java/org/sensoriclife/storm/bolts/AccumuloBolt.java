package org.sensoriclife.storm.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import java.io.IOException;
import java.util.Map;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.sensoriclife.Config;
import org.sensoriclife.Logger;
import org.sensoriclife.db.Accumulo;
import org.sensoriclife.util.Helpers;

/**
 *
 * @author jnphilipp
 * @version 0.1.0
 */
public class AccumuloBolt extends BaseRichBolt {
	private OutputCollector collector;
	private Map<String, String> confs;

	public AccumuloBolt() {
		this.confs = Config.toMap();
	}

	public AccumuloBolt(Map<String, String> confs) {
		this.confs = confs;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {}

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		Logger.debug(AccumuloBolt.class, "AccumuloBolt got tuple: ", input.toString());

		try {
			if ( (((this.confs.containsKey("accumulo.name") && !this.confs.get("accumulo.name").isEmpty()) || (this.confs.containsKey("accumulo.zooServers") && !this.confs.get("accumulo.zooServers").isEmpty())) || (this.confs.containsKey("accumulo.user") && !this.confs.get("accumulo.user").isEmpty())) || (this.confs.containsKey("accumulo.password") && !this.confs.get("accumulo.password").isEmpty()) )
				Accumulo.getInstance().connect(this.confs.get("accumulo.name"), this.confs.get("accumulo.zooServers"), this.confs.get("accumulo.user"), this.confs.get("accumulo.password"));
		}
		catch ( AccumuloException | AccumuloSecurityException e ) {
			Logger.error(AccumuloBolt.class, "Error while connecting to accumulo.", e.toString());
		}

		byte[] rowid = null, family = null, qualifier = null, value = null;
		long timestamp = 0;

		try {
			rowid = Helpers.toByteArray(input.getStringByField("rowid"));
			family = Helpers.toByteArray(input.getStringByField("family"));
			qualifier = Helpers.toByteArray(input.getStringByField("qualifier"));
			timestamp = input.getLongByField("timestamp");
			value = (byte[])input.getValueByField("value");
		}
		catch ( IOException e ) {
			Logger.error(AccumuloBolt.class, e.toString());
		}

		try {
			Accumulo.getInstance().addMutation(this.confs.get("accumulo.table_name"), rowid, family, qualifier, timestamp, value);
		}
		catch ( MutationsRejectedException | TableNotFoundException e ) {
			Logger.error(AccumuloBolt.class, "Error while writing to accumulo.", e.toString());
			this.collector.fail(input);
		}

		this.collector.ack(input);
	}
}