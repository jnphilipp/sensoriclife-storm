package org.sensoriclife.storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.sensoriclife.Logger;
import org.sensoriclife.db.Accumulo;
import org.sensoriclife.storm.bolts.AccumuloBolt;
import org.sensoriclife.storm.bolts.ElectricityBolt;
import org.sensoriclife.storm.bolts.WorldBolt;

/**
 *
 * @author jnphilipp
 * @version 0.0.1
 */
public class App {
	public static void main(String[] args) {
		Logger.getInstance();

		String name = "";
		boolean debug = false;
		List<String> l = Arrays.asList(args);
		Iterator<String> it = l.iterator();

		while ( it.hasNext() ) {
			switch ( it.next() ) {
				case "debug":
					debug = true;
				break;
				case "name":
					name = it.next();
					break;
			}
		}
		
		TopologyBuilder builder = new TopologyBuilder();
		builder.setBolt("world", new WorldBolt(), 1).shuffleGrouping("world_generator");
    builder.setBolt("electricity", new ElectricityBolt(), 10).shuffleGrouping("electricity_generator");
    builder.setBolt("accummolo", new AccumuloBolt(), 10).shuffleGrouping("electricity");

		Config conf = new Config();
		conf.setDebug(debug);

		if ( !debug ) {
			try {
				Accumulo.getInstance();
				Accumulo.getInstance().connect("", "", "", "");
			}
			catch ( AccumuloException | AccumuloSecurityException e ) {
				Logger.error("Error wihle connection to accumulo.", e.toString());
			}

			try {
				conf.setNumWorkers(3);
				StormSubmitter.submitTopology(name, conf, builder.createTopology());
			}
			catch ( AlreadyAliveException | InvalidTopologyException e ) {
				Logger.error("Error while submitting topology.", e.toString());
			}
		}
		else {
			try {
				Accumulo.getInstance();
				Accumulo.getInstance().connect();
			}
			catch ( AccumuloException | AccumuloSecurityException e ) {
				Logger.error("Error while creating mock instance.", e.toString());
			}

			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("test", conf, builder.createTopology());
			Utils.sleep(10000);
			cluster.killTopology("test");
			cluster.shutdown();
		}
	}
}