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
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.TimeZone;
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
public class HeatingBoltTest implements Serializable {
	private class TestHeatingSpout extends BaseRichSpout {
		private SpoutOutputCollector collector;
		private Random random;

		public TestHeatingSpout() {
			this.random = new Random(System.currentTimeMillis());
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("heating"));
		}

		@Override
		public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
			this.collector = collector;
		}

		@Override
		public void nextTuple() {
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss z");
			sdf.setTimeZone(TimeZone.getTimeZone("UTC"));

			String xml="<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?><heating>";

			for ( int i = 0; i < this.random.nextInt(6); i++ )
				xml += "<id>" + this.random.nextLong() + "</id><meter>" + this.random.nextFloat() + "</meter>";
			
			xml += "<time>"+ sdf.format(new Date(System.currentTimeMillis())) +"</time><heating>";
			this.collector.emit(new Values(xml));

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
		builder.setSpout("heatinggenerator", new TestHeatingSpout(), 1);
		builder.setBolt("heatingbolt", new HeatingBolt(), 1).shuffleGrouping("heatinggenerator");
		builder.setBolt("accumulobolt", new AccumuloBolt(), 1).shuffleGrouping("heatingbolt");

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