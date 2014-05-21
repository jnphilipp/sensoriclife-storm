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
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.sensoriclife.Logger;
import org.sensoriclife.util.Helpers;

/**
 *
 * @author jnphilipp
 * @version 0.0.3
 */
public class HeatingBolt extends BaseRichBolt {
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
		Logger.debug(HeatingBolt.class, "Reciving data:", input.toString());

		String heating = input.getStringByField("heating");

		long timestamp = 0;
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss z");
		sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
		try {
			Date date = sdf.parse(Helpers.get_tag_content_first("time", heating));
			timestamp = date.getTime();
		}
		catch ( java.text.ParseException e ) {
			Logger.error(ElectricityBolt.class, "Error while parsing time.", e.toString());
		}

		Matcher m = Pattern.compile("<id>(\\d+)</id><meter>(\\d+.\\d+)</meter>").matcher(heating);
		while ( m.find() ) {
			String id = m.group(1);
			String meter = m.group(2);

			byte[] value = null;
			try {
				value = Helpers.toByteArray(Float.parseFloat(meter));
			}
			catch ( IOException e ) {
				Logger.error(HeatingBolt.class, e.toString());
			}

		Values values = new Values(id + "_he", "device", "amount", timestamp, value);
		this.collector.emit(input, values);
		}
		this.collector.ack(input);
	}
}