package org.sensoriclife.storm.bolts;


import static org.junit.Assert.assertEquals;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.junit.Test;
import org.sensoriclife.Logger;
import org.sensoriclife.db.Accumulo;

/**
 *
 * @author jnphilipp
 * @version 0.0.1
 */
public class WorldBoltTest implements Serializable {
	private class TestSpout extends BaseRichSpout {
		private SpoutOutputCollector collector;
		private Random random;

		public TestSpout() {
			this.random = new Random(System.currentTimeMillis());
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("user", "billing_address", "other_addresses", "electricity_id", "hotwater_id","coldwater_id", "heating_ids"));
		}

		@Override
		public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
			this.collector = collector;
		}

		@Override
		public void nextTuple() {
			String name = String.valueOf(this.random.nextLong());
			String address = String.valueOf(this.random.nextLong());
			List<String> other = new ArrayList<>();
			int electricity_id = this.random.nextInt();
			int hotwater_id = this.random.nextInt();
			int coldwater_id = this.random.nextInt();
			int[] heating_ids = new int[this.random.nextInt(5)];

			for ( int i = 0; i < this.random.nextInt(5); i++ )
				other.add(String.valueOf(this.random.nextLong()));

			for ( int i = 0; i < heating_ids.length; i++ )
				heating_ids[i] = this.random.nextInt();

			this.collector.emit(new Values(name, address, other, electricity_id, hotwater_id,coldwater_id, heating_ids));
		}
	}

	/**
	 * Test WorldBolt.
	 */
	@Test
	public void testWorldBolt() {
		Logger.getInstance();

		try {
			Accumulo.getInstance();
			Accumulo.getInstance().connect();
			Accumulo.getInstance().createTable("users", false);
			Accumulo.getInstance().createTable("residentialUnit", false);
			Accumulo.getInstance().createTable("sensoriclife", false);
		}
		catch ( AccumuloException | AccumuloSecurityException e ) {
			Logger.error(WorldBoltTest.class, "Error while connecting to accumulo.", e.toString());
		}
		catch ( TableExistsException e ) {
			Logger.error(WorldBoltTest.class, "Error while creating table.", e.toString());
		}

		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("world_generator", new TestSpout());
		builder.setBolt("world", new WorldBolt(), 1).shuffleGrouping("world_generator");

		Config conf = new Config();
		conf.setDebug(true);

		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("test", conf, builder.createTopology());
		Utils.sleep(10000);
		cluster.killTopology("test");
		cluster.shutdown();
	}

	@Test
	public void testConstructor() {
		Logger.getInstance();

		new WorldBolt();
		assertEquals(WorldBolt.getCount(), 0);

		try {
			Accumulo.getInstance();
			Accumulo.getInstance().connect();
			Accumulo.getInstance().createTable("sensoriclife", false);
			Accumulo.getInstance().write("sensoriclife", "1_el", "user", "id", "1");
			Accumulo.getInstance().write("sensoriclife", "0_wh", "user", "id", "1");
			Accumulo.getInstance().write("sensoriclife", "0_wc", "user", "id", "1");
			Accumulo.getInstance().write("sensoriclife", "1_wc", "user", "id", "2");
		}
		catch ( AccumuloException | AccumuloSecurityException e ) {
			Logger.error(WorldBoltTest.class, "Error while connecting to accumulo.", e.toString());
		}
		catch ( TableExistsException e ) {
			Logger.error(WorldBoltTest.class, "Error while creating table.", e.toString());
		}
		catch ( TableNotFoundException e ) {
			Logger.error(WorldBoltTest.class, "Error while writing mutations.", e.toString());
		}

		new WorldBolt();
		assertEquals(3, WorldBolt.getCount());

		try {
			Accumulo.getInstance().write("sensoriclife", "3_el", "user", "id", "3");
			Accumulo.getInstance().write("sensoriclife", "2_wh", "user", "id", "5");
			Accumulo.getInstance().write("sensoriclife", "2_wc", "user", "id", "1");
			Accumulo.getInstance().write("sensoriclife", "6_wc", "user", "id", "6");
		}
		catch ( MutationsRejectedException | TableNotFoundException e ) {
			Logger.error(WorldBoltTest.class, "Error while writing mutations.", e.toString());
		}

		new WorldBolt();
		assertEquals(7, WorldBolt.getCount());
	}
}