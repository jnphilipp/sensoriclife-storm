package org.sensoriclife.storm.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import java.util.Map;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.sensoriclife.Logger;
import org.sensoriclife.db.Accumulo;

/**
 *
 * @author jnphilipp
 * @version 0.0.1
 */
public class AccumuloBolt extends BaseRichBolt {
	private OutputCollector collector;

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {}

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		Logger.debug(AccumuloBolt.class, "AccumuloBolt got tuple: ", input.toString());

		String rowid = input.getStringByField("rowid");
		String family = input.getStringByField("family");
		String qualifier = input.getStringByField("qualifier");
		long timestamp = input.getLongByField("timestamp");
		String value = input.getStringByField("value");

		try {
			Accumulo.getInstance().write("sensoriclife", rowid, family, qualifier, timestamp, value);
		}
		catch ( MutationsRejectedException | TableNotFoundException e ) {
			Logger.error(AccumuloBolt.class, "Error while writing to accumulo.", e.toString());
			this.collector.fail(input);
		}

		this.collector.ack(input);
	}
}