package org.sensoriclife.storm.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.sensoriclife.Logger;

/**
 *
 * @author jnphilipp
 * @version 0.0.2
 */
public class ElectricityBolt extends BaseRichBolt {
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
		Logger.debug(ElectricityBolt.class, "Reciving data:", input.toString());

		try {
			Object electricity = new JSONParser().parse(input.getStringByField("electricity"));
			if ( electricity instanceof JSONObject )
				this.collector.emit(input, this.convertJSON((JSONObject)electricity));
			else if ( electricity instanceof JSONArray )
				for ( Object obj : ((JSONArray)electricity).toArray() )
					this.collector.emit(input, this.convertJSON((JSONObject)obj));
			this.collector.ack(input);
		}
		catch ( ParseException e ) {
			Logger.error(ElectricityBolt.class, "Error while parsing JSON.", e.toString());
		}
	}

	private Values convertJSON(JSONObject obj) {
		String value = obj.get("value").toString();
		String time = obj.get("time").toString();
		String id = obj.get("id").toString();

		long timestamp = 0;
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss z");
		try {
			Date date = sdf.parse(time);
			timestamp = date.getTime();
		}
		catch ( java.text.ParseException e ) {
			Logger.error(ElectricityBolt.class, "Error while parsing time.", e.toString());
		}

		Values values = new Values(id + "_el", "device", "amount", timestamp, value);
		return values;
	}
}