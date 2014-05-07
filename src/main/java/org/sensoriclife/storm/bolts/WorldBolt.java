package org.sensoriclife.storm.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import java.security.NoSuchAlgorithmException;
import java.util.Map;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Value;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.sensoriclife.Logger;
import org.sensoriclife.db.Accumulo;
import org.sensoriclife.util.Helpers;

/**
 *
 * @author jnphilipp
 * @version 0.0.1
 */
public class WorldBolt extends BaseRichBolt {
	private OutputCollector collector;

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {}

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		if ( input.contains("user") ) {
			String name = input.getStringByField("user");
			String billingAddress = input.getStringByField("billing_address");
			String otherAddresses = input.getStringByField("other_addresses");

			try {
				JSONObject user = new JSONObject();
				JSONArray ja = new JSONArray();
				for ( String s : otherAddresses.split(";") )
					ja.add(new JSONObject().put("address", s));
				user.put("name", name);
				user.put("billing_address", billingAddress);
				user.put("otherAddresses", ja);

				Value value = new Value(user.toJSONString().getBytes());
				Accumulo.getInstance().write("users", Helpers.getSHA512(name + "#" + billingAddress), "user", null, value);
			}
			catch ( NoSuchAlgorithmException e ) {
				Logger.error(WorldBolt.class, e.toString());
			}
			catch ( TableNotFoundException | MutationsRejectedException e ) {
				Logger.error(WorldBolt.class, "Error while writing to accumulo.", e.toString());
			}
		}
		else if ( input.contains("electricity_id") ) {
			String electricityId = input.getStringByField("electricity_id");
			String address = input.getStringByField("address");

			try {
				JSONObject unit = new JSONObject();
				unit.put("electricity_id", electricityId);

				Value value = new Value(unit.toJSONString().getBytes());
				Accumulo.getInstance().write("residentialUnit", address, "residentialUnit", null, value);
			}
			catch ( TableNotFoundException | MutationsRejectedException e ) {
				Logger.error(WorldBolt.class, "Error while writing to accumulo.", e.toString());
			}
		}
	}
}