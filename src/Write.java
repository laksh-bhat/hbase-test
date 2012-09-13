import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;

public class Write {
	static public void main(String args[]) throws InterruptedException {
		long start, end;
		if (args.length < 3) {
			System.out.println("arguments: host table_name type");
			System.out.println("type: throughput, latency");
			return;
		}
		
		String host = args[0];
		String tableName = args[1];
		String type = args[2];
		
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
			if (admin.tableExists(tableName)) {
				admin.disableTable(tableName);
				admin.deleteTable(tableName);
				System.out.println("delete table " + tableName);
			}
			
			admin.createTable(tableDesc);
			admin.disableTable(tableName);
			HColumnDescriptor cf1 = new HColumnDescriptor(columnFamily);
			admin.addColumn(tableName, cf1);      // adding new ColumnFamily
			admin.enableTable(tableName);
			admin.close();
		} catch (IOException e) {
			System.err.println("can't create table " + tableName);
			e.printStackTrace();
		}
		end = System.currentTimeMillis();
		System.out.println("creating a table takes: " + (((double) end - start) / 1000));

		// insert rows
		start = System.currentTimeMillis();
		long bytes = 0;
		try {
			System.out.println("add rows");
			HTable table = new HTable(conf, tableName);
			if (type.equals("throughput")) {
				for (int i = 0; i < 1000; i++) {
					String rowKey = new Integer(i).toString();
					Put put = new Put(rowKey.getBytes());
					put.add(columnFamily.getBytes(), 
							new Integer(i).toString().getBytes(),
							new byte[1024 * 1024]);
					bytes += 1024 * 1024;
					table.put(put);
				}
			}
			else if (type.equals("latency")) {
				for (int i = 1000; i < 2000; i++) {
					String rowKey = new Integer(i).toString();
					Put put = new Put(rowKey.getBytes());
					put.add(columnFamily.getBytes(), 
							new Integer(i).toString().getBytes(),
							new byte[1]);
					bytes++;
					table.put(put);
				}
			}
			table.close();

		} catch (IOException e) {
			e.printStackTrace();
		}
		end = System.currentTimeMillis();
		System.out.println("inserting " + bytes + "B takes: " + (((double) end - start) / 1000));

	}
}
