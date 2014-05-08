package org.sensoriclife.storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;
import java.io.IOException;
import java.util.Properties;
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
	private static Properties properties;

	public static void main(String[] args) {
		Logger.getInstance();

		App.loadProperties();

		TopologyBuilder builder = new TopologyBuilder();
		builder.setBolt("world", new WorldBolt(), 1).shuffleGrouping("world_generator");
    builder.setBolt("electricity", new ElectricityBolt(), 10).shuffleGrouping("electricity_generator");
    builder.setBolt("accummolo", new AccumuloBolt(), 10).shuffleGrouping("electricity");

		Config conf = new Config();
		conf.setDebug(App.getBooleanProperty("storm.debug"));

		if ( !App.getBooleanProperty("storm.debug") ) {
			try {
				Accumulo.getInstance();
				Accumulo.getInstance().connect("", "", "", "");
			}
			catch ( AccumuloException | AccumuloSecurityException e ) {
				Logger.error("Error wihle connection to accumulo.", e.toString());
			}

			try {
				conf.setNumWorkers(3);
				StormSubmitter.submitTopology(App.getProperty("storm.name"), conf, builder.createTopology());
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

	/**
	 * @param key key
	 * @return values of the given key
	 */
	public static String getProperty(String key) {
		switch (key) {
		case "storm.debug":
			return App.properties.getProperty(key, "true");
		default:
			return App.properties.getProperty(key, "");
		}
	}

	/**
	 * Returns the value of the given key as boolean;
	 * @param key key
	 * @return <code>true</code> or <code>false</code>
	 */
	public static boolean getBooleanProperty(String key) {
		return Boolean.valueOf(App.getProperty(key));
	}

	/**
	 * Returns the value of the given key as integer.
	 * @param key key
	 * @return integer value
	 */
	public static int getIntegerProperty(String key) {
		return Integer.parseInt(App.getProperty(key));
	}

	private static void loadProperties() {
		try {
			App.properties = new Properties();
			App.properties.load(App.class.getResourceAsStream("/config.properties"));
		}
		catch ( IOException e ) {
			Logger.error("Error while loading config file.", e.toString());
			System.exit(1);
		}
	}
}