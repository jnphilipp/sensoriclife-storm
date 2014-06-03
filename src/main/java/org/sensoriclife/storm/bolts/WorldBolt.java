package org.sensoriclife.storm.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.commons.lang.ArrayUtils;
import org.sensoriclife.Config;
import org.sensoriclife.Logger;
import org.sensoriclife.db.Accumulo;
import org.sensoriclife.util.Helpers;

/**
 *
 * @author jnphilipp
 * @version 0.1.1
 */
public class WorldBolt extends BaseRichBolt {
	private static long count = 0;
	private OutputCollector collector;

	public WorldBolt() throws TableNotFoundException {
		this.init(Accumulo.getInstance(), Config.getProperty("accumulo.table_name"));
	}

	public WorldBolt(Accumulo accumulo, String table) throws TableNotFoundException {
		this.init(accumulo, table);
	}

	private void init(Accumulo accumulo, String table) throws TableNotFoundException {
		Iterator<Entry<Key,Value>> iterator = accumulo.scanColumns(table, "user", "id");

		long tmp = 0;
		while ( iterator.hasNext() ) {
			Entry<Key, Value> entry = iterator.next();
			if ( Long.valueOf(entry.getValue().toString()) > tmp )
				tmp = Long.valueOf(entry.getValue().toString());
		}

		if ( count < tmp )
			count = tmp + 1;
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
		String others = input.getStringByField("other_addresses");
		long electricity = input.getLongByField("electricity_id");
		long hotwater = input.getLongByField("hotwater_id");
		long coldwater = input.getLongByField("coldwater_id");
		long[] heatings = (long[])input.getValueByField("heating_ids");
		byte[] billing = null;

		try {
			billing = Helpers.toByteArray(input.getStringByField("billing_address"));
		}
		catch ( IOException e ) {
			Logger.error(WorldBolt.class, e.toString());
		}

		this.collector.emit(input, new Values(electricity + "_el", "residential", "id", System.currentTimeMillis(), billing));
		this.collector.emit(input, new Values(hotwater + "_wh", "residential", "id", System.currentTimeMillis(), billing));
		this.collector.emit(input, new Values(coldwater + "_wc", "residential", "id", System.currentTimeMillis(), billing));

		for ( long heating : heatings )
			this.collector.emit(input, new Values(heating + "_he", "residential", "id", System.currentTimeMillis(), billing));

		if ( !user.isEmpty() ) {
			try {
				byte[] c = Helpers.toByteArray(count++);
				byte[] u = Helpers.toByteArray(user);

				byte[] value = ArrayUtils.addAll(c, u);

				this.collector.emit(input, new Values(electricity + "_el", "user", "id", System.currentTimeMillis(), value));
				this.collector.emit(input, new Values(hotwater + "_wh", "user", "id", System.currentTimeMillis(), value));
				this.collector.emit(input, new Values(coldwater + "_wc", "user", "id", System.currentTimeMillis(), value));

				for ( long heating : heatings )
					this.collector.emit(input, new Values(heating + "_he", "user", "id", System.currentTimeMillis(), value));
			}
			catch ( IOException e ) {
				Logger.error(WorldBolt.class, e.toString());
			}

			byte[] all = null;
			try {
				all = Helpers.toByteArray(input.getStringByField("billing_address") + (others == null || others.isEmpty() ? "" : ";" + others));
			}
			catch ( IOException e ) {
				Logger.error(WorldBolt.class, e.toString());
			}

			this.collector.emit(input, new Values(electricity + "_el", "user", "residential", System.currentTimeMillis(), all));
			this.collector.emit(input, new Values(hotwater + "_wh", "user", "residential", System.currentTimeMillis(), all));
			this.collector.emit(input, new Values(coldwater + "_wc", "user", "residential", System.currentTimeMillis(), all));

			for ( long heating : heatings )
				this.collector.emit(input, new Values(heating + "_he", "user", "residential", System.currentTimeMillis(), all));
		}

		this.collector.ack(input);
	}
}