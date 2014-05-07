package org.sensoriclife.storm.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import java.text.SimpleDateFormat;
import java.util.Map;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Value;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
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

		if ( input.contains("cleaned_electricity") ) {
			try {
				JSONObject data = (JSONObject)new JSONParser().parse(input.getStringByField("cleaned_electricity"));

				SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd-hh:mm:ss-z");
				long timestamp = 0;
				try {
					timestamp = sdf.parse(data.get("timestamp").toString()).getTime();
				}
				catch ( java.text.ParseException e ) {
					Logger.error(AccumuloBolt.class, "Error while parsing time.", e.toString());
				}

				Value value = new Value(data.get("value").toString().getBytes());
				try {
					Accumulo.getInstance().write("electricity_consumption", data.get("id").toString(), "electricity", null, timestamp, value);
				}
				catch ( MutationsRejectedException | TableNotFoundException e ) {
					Logger.error(AccumuloBolt.class, "Error while writing to accumulo.", e.toString());
				}
			}
			catch ( ParseException e ) {
				Logger.error(AccumuloBolt.class, "Error while parsing JSON.", e.toString());
			}
		}
	}
}