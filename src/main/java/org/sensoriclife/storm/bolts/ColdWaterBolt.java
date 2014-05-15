package org.sensoriclife.storm.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import java.util.Map;

/**
 *
 * @author jnphilipp
 * @version 0.1.0
 */
public class ColdWaterBolt extends BaseRichBolt {
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
		int id = input.getIntegerByField("id");
		String value = String.valueOf(input.getFloatByField("value"));
		long timestamp = input.getLongByField("time");

		Values values = new Values(id + "_wc", "device", "amount", timestamp, value);
		this.collector.emit(input, values);
		this.collector.ack(input);
	}
}