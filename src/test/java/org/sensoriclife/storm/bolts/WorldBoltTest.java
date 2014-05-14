package org.sensoriclife.storm.bolts;


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
import java.util.Map;
import java.util.Random;
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
public class WorldBoltTest implements Serializable {
	private class TestSpout extends BaseRichSpout {
		private SpoutOutputCollector collector;
		private Random random;

		public TestSpout() {
			this.random = new Random(System.currentTimeMillis());
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("user", "billing_address", "other_addresses"));
		}

		@Override
		public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
			this.collector = collector;
		}

		@Override
		public void nextTuple() {
			long name = this.random.nextLong();
			long address = this.random.nextLong();
			long[] other = new long[this.random.nextInt(5)];

			for ( int i = 0; i < other.length; i++ )
				other[i] = this.random.nextLong();

			this.collector.emit(new Values(name, address, other));
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
}