package org.sensoriclife.storm.bolts;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.junit.Test;
import org.sensoriclife.Logger;
import org.sensoriclife.db.Accumulo;
import org.sensoriclife.util.Helpers;

/**
 *
 * @author jnphilipp
 * @version 0.0.1
 */
public class WorldBoltTest implements Serializable {
	private static boolean created = false;

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
			if ( created )
				return;

			for ( int j = 0; j < 10; j++ ) {
				String name = String.valueOf(this.random.nextLong());
				String address = String.valueOf(this.random.nextLong());
				List<String> other = new ArrayList<>();
				long electricity_id = this.random.nextLong();
				long hotwater_id = this.random.nextLong();
				long coldwater_id = this.random.nextLong();
				long[] heating_ids = new long[this.random.nextInt(5)];

				for ( int i = 0; i < this.random.nextInt(5); i++ )
					other.add(String.valueOf(this.random.nextLong()));

				for ( int i = 0; i < heating_ids.length; i++ )
					heating_ids[i] = this.random.nextLong();

				this.collector.emit(new Values(name, address, Helpers.join(other, ";"), electricity_id, hotwater_id,coldwater_id, heating_ids));
			}

			created = true;
		}
	}

	@Test
	public void testWorldBolt() throws AccumuloException, AccumuloSecurityException, TableExistsException, TableNotFoundException {
		Logger.getInstance();
		org.sensoriclife.Config.getInstance().getProperties().setProperty("accumulo.table_name", "sensoriclife");

		Accumulo.getInstance();
		Accumulo.getInstance().connect();
		Accumulo.getInstance().createTable("sensoriclife", false);

		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("worldgenerator", new TestSpout(), 1);
		builder.setBolt("worldbolt", new WorldBolt(), 1).shuffleGrouping("worldgenerator");
		builder.setBolt("accumulobolt", new AccumuloBolt(), 1).shuffleGrouping("worldbolt");

		Config conf = new Config();
		conf.setDebug(true);

		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("test", conf, builder.createTopology());
		Utils.sleep(30000);
		cluster.killTopology("test");
		cluster.shutdown();

		Accumulo.getInstance().closeBashWriter("sensoriclife");

		Iterator<Entry<Key, Value>> entries = Accumulo.getInstance().scanAll("sensoriclife");
		int i = 0;
		for ( ; entries.hasNext(); ++i ) {entries.next();}
		assertTrue(i > 0);

		Accumulo.getInstance().deleteTable("sensoriclife");
	}

	@Test
	public void testConstructor() throws AccumuloException, AccumuloSecurityException, TableExistsException, MutationsRejectedException, TableNotFoundException {
		Logger.getInstance();
		org.sensoriclife.Config.getInstance().getProperties().setProperty("accumulo.table_name", "sensoriclife");

		Accumulo.getInstance();
		Accumulo.getInstance().connect();
		Accumulo.getInstance().createTable("sensoriclife", false);

		new WorldBolt();
		assertEquals(WorldBolt.getCount(), 0);

		Accumulo.getInstance().addMutation("sensoriclife", "1_el", "user", "id", "1".getBytes());
		Accumulo.getInstance().addMutation("sensoriclife", "0_wh", "user", "id", "1".getBytes());
		Accumulo.getInstance().addMutation("sensoriclife", "0_wc", "user", "id", "1".getBytes());
		Accumulo.getInstance().addMutation("sensoriclife", "1_wc", "user", "id", "2".getBytes());

		new WorldBolt();
		assertEquals(3, WorldBolt.getCount());

		Accumulo.getInstance().addMutation("sensoriclife", "3_el", "user", "id", "3".getBytes());
		Accumulo.getInstance().addMutation("sensoriclife", "2_wh", "user", "id", "5".getBytes());
		Accumulo.getInstance().addMutation("sensoriclife", "2_wc", "user", "id", "1".getBytes());
		Accumulo.getInstance().addMutation("sensoriclife", "6_wc", "user", "id", "6".getBytes());

		new WorldBolt();
		assertEquals(7, WorldBolt.getCount());
		Accumulo.getInstance().deleteTable("sensoriclife");
	}
}