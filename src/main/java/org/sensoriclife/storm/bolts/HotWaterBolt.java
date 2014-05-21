package org.sensoriclife.storm.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import java.io.IOException;
import java.util.Map;
import org.sensoriclife.Logger;
import org.sensoriclife.util.Helpers;

/**
 *
 * @author jnphilipp
 * @version 0.1.2
 */
public class HotWaterBolt extends BaseRichBolt {
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
		Logger.debug(HotWaterBolt.class, "Reciving data:", input.toString());

		long id = input.getLongByField("hotwater_id");
		long timestamp = input.getLongByField("time");
		byte[] value = null;

		try {
			value = Helpers.toByteArray(input.getFloatByField("hotWaterMeter"));
		}
		catch ( IOException e ) {
			Logger.error(ColdWaterBolt.class, e.toString());
		}

		Values values = new Values(id + "_wh", "device", "amount", timestamp, value);
		this.collector.emit(input, values);
		this.collector.ack(input);
	}
}