package org.sensoriclife.db;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.Iterator;
import java.util.Map.Entry;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.junit.Test;

/**
 *
 * @author jnphilipp
 * @version 0.0.1
 */
public class AccumuloTest {
	@Test
	public void testAccumulo() {
		try {
			Accumulo accumulo = Accumulo.getInstance();
			accumulo.connect();
			accumulo.createTable("electricity_consumption");
			
			Value value = new Value("0".getBytes());
			accumulo.write("electricity_consumption", "1", "electricity", "", value);

			value = new Value("1".getBytes());
			accumulo.write("electricity_consumption", "2", "electricity", "", value);

			value = new Value("5".getBytes());
			accumulo.write("electricity_consumption", "3", "electricity", "", value);

			value = new Value("5".getBytes());
			accumulo.write("electricity_consumption", "4", "electricity", "", value);

			Iterator<Entry<Key, Value>> entries = accumulo.scannByKey("electricity_consumption", "public", new Range("2", "3"));
			int i = 0;
			while ( entries.hasNext() ) {
				Entry<Key, Value> entry = entries.next();
				i++;
			}

			assertEquals(i, 2);
		}
		catch ( AccumuloException | AccumuloSecurityException e ) {
			fail("Couldn't connect to accumulo: " + e.toString());
		}
		catch ( TableNotFoundException | TableExistsException e ) {
			fail("Error: " + e.toString());
		}
	}
}