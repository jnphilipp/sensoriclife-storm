package org.sensoriclife.storm.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import java.util.List;
import java.util.Map;

/**
 *
 * @author jnphilipp
 * @version 0.0.1
 */
public class WorldBolt extends BaseRichBolt {
	private static long count = 0;
	private OutputCollector collector;

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
		//declarer.declare(new Fields("user", "billing_address", "other_addresses", "electricity_id", "water_id", "heating_id"));
		String user = input.getStringByField("user");
		String billing = input.getStringByField("billing_address");
		List<String> other = (List<String>)input.getValueByField("other_addresses");
		int electricity = input.getIntegerByField("electricity_id");
		int hotwater = input.getIntegerByField("hotwater_id");
		int coldwater = input.getIntegerByField("coldwater_id");
		int[] heatings = (int[])input.getValueByField("heating_id");

		this.collector.emit(input, new Values(electricity + "_el", "residential", "id", System.currentTimeMillis(), billing));
	}
}