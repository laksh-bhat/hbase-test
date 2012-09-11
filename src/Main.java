import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import org.apache.commons.math.util.OpenIntToDoubleHashMap.Iterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
public class Main {
	static public void main(String args[]) {
		String host = "localhost.60000";
		if (args.length > 0)
			host = args[0];
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.master", host);
		HBaseAdmin admin;
		try {
			admin = new HBaseAdmin(conf);
		} catch (MasterNotRunningException e) {
			e.printStackTrace();
			return;
		} catch (ZooKeeperConnectionException e) {
			e.printStackTrace();
			return;
		}    
		String tableName = "myTable";	
		String columnFamily = "test";
		HTableDescriptor tableDesc = new HTableDescriptor(tableName);
		try {
			System.out.println("create table " + tableName);
			admin.createTable(tableDesc);
			admin.disableTable(tableName);
			HColumnDescriptor cf1 = new HColumnDescriptor(columnFamily);
			admin.addColumn(tableName, cf1);      // adding new ColumnFamily
			admin.enableTable(tableName);
		} catch (IOException e) {
			System.err.println("can't create table " + tableName);
			e.printStackTrace();
		}
		// insert rows
		HTable table = null;
		try {
			System.out.println("add rows");
			table = new HTable(conf, tableName);
			for (int i = 0; i < 100; i++) {
				String rowKey = new Integer(i).toString();
				Put put = new Put(rowKey.getBytes());
				put.add(columnFamily.getBytes(), 
						new Integer(i).toString().getBytes(),
						new Integer(i).toString().getBytes());
				table.put(put);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		if (table == null) {
			System.err.println("we can't create table instance");
			return;
		}
		// search for rows
		try {
			System.out.println("get rows");
			for (int i = 0; i < 100; i++) {
				String rowKey = new Integer(i).toString();
				Get get = new Get(rowKey.getBytes());
				Result result = table.get(get);
				NavigableMap<byte[], byte[]> map = result.getFamilyMap(columnFamily.getBytes());
				for (Map.Entry<byte[], byte[]> entry = map.pollFirstEntry(); entry != null; ) {
					Integer key = new Integer(new String(entry.getKey()));
					Integer value = new Integer(new String(entry.getValue()));
					System.out.println("Get row: " + Bytes.toString(result.getRow())
							+ " with value: " + value);
					entry = map.pollFirstEntry();
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		try {
			System.out.println("range query");
			Scan scan = new Scan(new Integer(4).toString().getBytes(),
					new Integer(50).toString().getBytes());
			ResultScanner scanner = table.getScanner(scan);
			// The other approach is to use a foreach loop. Scanners are iterable!
			for (Result result = scanner.next(); result != null; result = scanner.next()) {
				NavigableMap<byte[], byte[]> map = result.getFamilyMap(columnFamily.getBytes());
				for (Map.Entry<byte[], byte[]> entry = map.pollFirstEntry(); entry != null; ) {
					Integer key = new Integer(new String(entry.getKey()));
					Integer value = new Integer(new String(entry.getValue()));
					System.out.println("Found row: " + Bytes.toString(result.getRow())
							+ " with value: " + value);
					entry = map.pollFirstEntry();
				}
			}
		} catch(IOException e) {
			e.printStackTrace();
		}
		try {
			table.close();
			admin.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
