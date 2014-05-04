package org.sensoriclife.storm.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import java.util.Map;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.sensoriclife.Logger;

/**
 *
 * @author jnphilipp
 * @version 0.0.1
 */
public class ElectricityBolt extends BaseRichBolt {
	private OutputCollector collector;

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("cleaned_electricity"));
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		try {
			Object electricity = new JSONParser().parse(input.getStringByField("electricity"));
			if ( electricity instanceof JSONObject )
				this.collector.emit(input, new Values(this.convertJSON((JSONObject)electricity).toJSONString()));
			else if ( electricity instanceof JSONArray )
				for ( Object obj : ((JSONArray)electricity).toArray() )
					this.collector.emit(input, new Values(this.convertJSON((JSONObject)obj).toJSONString()));
			this.collector.ack(input);
		}
		catch ( ParseException e ) {
			Logger.error(ElectricityBolt.class, "Error while parsing JSON.", e.toString());
		}
	}

	private JSONObject convertJSON(JSONObject obj) {
		String value = obj.get("value").toString();
		String time = obj.get("time").toString();
		String id = obj.get("id").toString();

		JSONObject converted = new JSONObject();
		converted.put("electricity_id", id);
		converted.put("timestamp", time);
		converted.put("counter_value", value);
		return converted;
	}
}