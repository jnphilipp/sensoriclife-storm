package org.sensoriclife.storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;
import org.sensoriclife.Logger;
import org.sensoriclife.storm.bolts.AccumuloBolt;
import org.sensoriclife.storm.bolts.ElectricityBolt;

/**
 *
 * @author jnphilipp
 * @version 0.0.1
 */
public class App {
	public static void main(String[] args) {
		Logger.getInstance();

		TopologyBuilder builder = new TopologyBuilder();
    builder.setBolt("electricity", new ElectricityBolt(), 10).shuffleGrouping("electricity_generator");
    builder.setBolt("accummolo", new AccumuloBolt(), 10).shuffleGrouping("electricity");

		Config conf = new Config();
		conf.setDebug(true);
		conf.setNumWorkers(2);

		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("test", conf, builder.createTopology());
		Utils.sleep(10000);
		cluster.killTopology("test");
		cluster.shutdown();
	}
}