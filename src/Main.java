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
		long start, end;
		String host = "localhost.60000";
		if (args.length > 0)
			host = args[0];
		String tableName = "myTable";
		if (args.length > 1)
			tableName = args[1];
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

		String columnFamily = "test";
		start = System.currentTimeMillis();
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
		end = System.currentTimeMillis();
		System.out.println("creating a table takes: " + (((double) end - start) / 1000));

		// insert rows
		HTable table = null;
		start = System.currentTimeMillis();
		try {
			System.out.println("add rows");
			table = new HTable(conf, tableName);
			for (int i = 0; i < 1000; i++) {
				String rowKey = new Integer(i).toString();
				Put put = new Put(rowKey.getBytes());
				put.add(columnFamily.getBytes(), 
						new Integer(i).toString().getBytes(),
						new byte[1024 * 1024]);
				table.put(put);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		end = System.currentTimeMillis();
		System.out.println("inserting 1GB takes: " + (((double) end - start) / 1000));

		start = System.currentTimeMillis();
		try {
			System.out.println("add rows");
			table = new HTable(conf, tableName);
			for (int i = 1000; i < 2000; i++) {
				String rowKey = new Integer(i).toString();
				Put put = new Put(rowKey.getBytes());
				put.add(columnFamily.getBytes(), 
						new Integer(i).toString().getBytes(),
						new byte[1]);
				table.put(put);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		end = System.currentTimeMillis();
		System.out.println("inserting 1GB takes: " + (((double) end - start) / 1000));

		try {
			table.close();
			admin.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
