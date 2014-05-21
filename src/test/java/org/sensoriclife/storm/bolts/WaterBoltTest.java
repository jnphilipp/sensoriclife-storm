package org.sensoriclife.storm.bolts;

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
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.junit.Test;
import org.sensoriclife.Logger;
import org.sensoriclife.db.Accumulo;

/**
 *
 * @author jnphilipp
 * @version 0.0.1
 */
public class WaterBoltTest implements Serializable {
	private class TestWaterSpout extends BaseRichSpout {
		private SpoutOutputCollector collector;
		private Random random;

		public TestWaterSpout() {
			this.random = new Random(System.currentTimeMillis());
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declareStream("hotwater", new Fields("hotwater_id", "hotWaterMeter", "time"));
			declarer.declareStream("coldwater", new Fields("coldwater_id", "coldWaterMeter", "time"));
		}

		@Override
		public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
			this.collector = collector;
		}

		@Override
		public void nextTuple() {
			long hotWaterId = this.random.nextLong();
			float hotWaterMeter = this.random.nextFloat();
			long timestamp = System.currentTimeMillis();
			this.collector.emit("hotwater", new Values(hotWaterId, hotWaterMeter, timestamp));

			long coldWaterId = this.random.nextLong();
			float coldWaterMeter = this.random.nextFloat();
			timestamp = System.currentTimeMillis();
			this.collector.emit("hotwater", new Values(hotWaterId, hotWaterMeter, timestamp));

			try {
				Thread.sleep(1000);
			}
			catch ( InterruptedException e ) {
				Logger.error(WaterBoltTest.class, e.toString());
			}
		}
	}

	@Test
	public void testBolt() throws AccumuloException, AccumuloSecurityException, TableExistsException, TableNotFoundException {
		Logger.getInstance();
		org.sensoriclife.Config.getInstance().getProperties().setProperty("accumulo.table_name", "sensoriclife");

		Accumulo.getInstance();
		Accumulo.getInstance().connect();
		Accumulo.getInstance().createTable("sensoriclife", false);

		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("watergenerator", new TestWaterSpout(), 1);
		builder.setBolt("hotwaterdbolt", new HotWaterBolt(), 1).shuffleGrouping("watergenerator", "hotwater");
		builder.setBolt("coldwaterdbolt", new ColdWaterBolt(), 1).shuffleGrouping("watergenerator", "coldwater");
		builder.setBolt("accumulobolt", new AccumuloBolt(), 1).shuffleGrouping("hotwaterdbolt").shuffleGrouping("coldwaterdbolt");

		Config conf = new Config();
		conf.setDebug(true);

		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("test", conf, builder.createTopology());
		Utils.sleep(10000);
		cluster.killTopology("test");
		cluster.shutdown();

		Iterator<Map.Entry<Key, Value>> entries = Accumulo.getInstance().scanAll("sensoriclife");
		int i = 0;
		for ( ; entries.hasNext(); ++i ) {entries.next();}
		assertTrue(i > 0);

		Accumulo.getInstance().deleteTable("sensoriclife");
	}
}