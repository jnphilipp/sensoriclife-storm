package org.sensoriclife.storm.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import org.sensoriclife.Logger;
import org.sensoriclife.util.Helpers;

/**
 *
 * @author jnphilipp
 * @version 0.1.3
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
		Logger.debug(ColdWaterBolt.class, "Reciving data:", input.toString());

		long id = input.getLongByField("coldwater_id");
		byte[] value = null;

		long timestamp = 0;
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss z");
		try {
			Date date = sdf.parse(input.getStringByField("time"));
			timestamp = date.getTime();
		}
		catch ( java.text.ParseException e ) {
			Logger.error(ElectricityBolt.class, "Error while parsing time.", e.toString());
		}

		try {
			value = Helpers.toByteArray(input.getFloatByField("coldWaterMeter"));
		}
		catch ( IOException e ) {
			Logger.error(ColdWaterBolt.class, e.toString());
		}

		Values values = new Values(id + "_wc", "device", "amount", timestamp, value);
		this.collector.emit(input, values);
		this.collector.ack(input);
	}
}