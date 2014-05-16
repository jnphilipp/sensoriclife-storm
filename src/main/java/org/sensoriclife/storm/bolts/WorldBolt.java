package org.sensoriclife.storm.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.sensoriclife.Logger;
import org.sensoriclife.db.Accumulo;
import org.sensoriclife.util.Helpers;

/**
 *
 * @author jnphilipp
 * @version 0.0.2
 */
public class WorldBolt extends BaseRichBolt {
	private static long count = 0;
	private OutputCollector collector;

	public WorldBolt() {
		try {
			Iterator<Entry<Key,Value>> iterator = Accumulo.getInstance().scanColumns("sensoriclife", "user", "id");

			long tmp = 0;
			while ( iterator.hasNext() ) {
				Entry<Key, Value> entry = iterator.next();
				if ( Long.valueOf(entry.getValue().toString()) > tmp )
					tmp = Long.valueOf(entry.getValue().toString());
			}

			if ( count < tmp )
				count = tmp + 1;
		}
		catch ( TableNotFoundException e ) {
			Logger.error(WorldBolt.class, "Error while loading number of users.", e.toString());
		}
	}

	public static long getCount() {
		return count;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("rowid", "family", "qualifier", "timestamp", "value"));
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		Logger.debug(WorldBolt.class, "Reciving data:", input.toString());

		String user = input.getStringByField("user");
		String billing = input.getStringByField("billing_address");
		List<String> others = (List<String>)input.getValueByField("other_addresses");
		int electricity = input.getIntegerByField("electricity_id");
		int hotwater = input.getIntegerByField("hotwater_id");
		int coldwater = input.getIntegerByField("coldwater_id");
		int[] heatings = (int[])input.getValueByField("heating_ids");

		this.collector.emit(input, new Values(electricity + "_el", "residential", "id", System.currentTimeMillis(), billing));
		this.collector.emit(input, new Values(hotwater + "_wh", "residential", "id", System.currentTimeMillis(), billing));
		this.collector.emit(input, new Values(coldwater + "_wc", "residential", "id", System.currentTimeMillis(), billing));

		for ( int heating : heatings )
			this.collector.emit(input, new Values(coldwater + "_he", "residential", "id", System.currentTimeMillis(), billing));

		if ( !user.isEmpty() ) {
			this.collector.emit(input, new Values(electricity + "_el", "user", "id", System.currentTimeMillis(), count + ";" + user));
			this.collector.emit(input, new Values(hotwater + "_wh", "user", "id", System.currentTimeMillis(), count + ";" + user));
			this.collector.emit(input, new Values(coldwater + "_wc", "user", "id", System.currentTimeMillis(), count + ";" + user));

			for ( int heating : heatings )
				this.collector.emit(input, new Values(coldwater + "_he", "user", "id", System.currentTimeMillis(), count + ";" + user));
		}

		String all = billing + (others.isEmpty() ? "" : ";" + Helpers.join(others, ";"));

		this.collector.emit(input, new Values(electricity + "_el", "user", "residential", System.currentTimeMillis(), all));
		this.collector.emit(input, new Values(hotwater + "_wh", "user", "residential", System.currentTimeMillis(), all));
		this.collector.emit(input, new Values(coldwater + "_wc", "user", "residential", System.currentTimeMillis(), all));

		for ( int heating : heatings )
			this.collector.emit(input, new Values(coldwater + "_he", "user", "residential", System.currentTimeMillis(), all));

		this.collector.ack(input);
	}
}