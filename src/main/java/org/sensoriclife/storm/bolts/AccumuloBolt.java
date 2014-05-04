package org.sensoriclife.storm.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import java.util.Map;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.sensoriclife.Logger;

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
		Logger.info(AccumuloBolt.class, "AccumuloBolt got tuple: ", input.toString());

		String instanceName = "sensoriclife";
		//String zooServers = "zooserver-one,zooserver-two";
		//Instance inst = new ZooKeeperInstance(instanceName, zooServers);
		Instance instance = new MockInstance();
		//Connector conn = inst.getConnector("user", new PasswordToken("passwd"));

		String rowID = "row1";
		String data = input.getStringByField("cleaned_electricity");
		ColumnVisibility colVis = new ColumnVisibility("public");
		long timestamp = System.currentTimeMillis();

		Value value = new Value("myValue".getBytes());

		Mutation mutation = new Mutation(rowID);
		mutation.put(data, (CharSequence)colVis, timestamp, value);
		BatchWriterConfig config = new BatchWriterConfig();
		config.setMaxMemory(10000000L);

		/*BatchWriter writer = conn.createBatchWriter("table", config);
		writer.add(mutation);
		writer.close();*/
	}
}