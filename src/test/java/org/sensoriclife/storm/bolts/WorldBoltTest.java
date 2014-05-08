package org.sensoriclife.storm.bolts;


import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableExistsException;
import org.junit.Test;
import org.sensoriclife.Logger;
import org.sensoriclife.db.Accumulo;

/**
 *
 * @author jnphilipp
 * @version 0.0.1
 */
public class WorldBoltTest {
	/**
	 * Test WorldBolt.
	 */
	@Test
	public void testWorldBolt() {
		Logger.getInstance();

		try {
			Accumulo.getInstance();
			Accumulo.getInstance().connect();
			Accumulo.getInstance().createTable("users");
			Accumulo.getInstance().createTable("residentialUnit");
		}
		catch ( AccumuloException | AccumuloSecurityException e ) {
			Logger.error(WorldBoltTest.class, "Error while connecting to accumulo.", e.toString());
		}
		catch ( TableExistsException e ) {
			Logger.error(WorldBoltTest.class, "Error while creating table.", e.toString());
		}

		TopologyBuilder builder = new TopologyBuilder();
		builder.setBolt("world", new WorldBolt(), 1).shuffleGrouping("world_generator");

		Config conf = new Config();
		conf.setDebug(true);

		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("test", conf, builder.createTopology());
		Utils.sleep(10000);
		cluster.killTopology("test");
		cluster.shutdown();
	}
}