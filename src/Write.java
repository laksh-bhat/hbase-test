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
	static final int numThreads = 10;
	
	static public void main(String args[]) throws InterruptedException {
		long start, end;
		if (args.length < 3) {
			System.out.println("arguments: host table_name type");
			System.out.println("type: throughput, latency");
			return;
		}
		
		final String host = args[0];
		final String tableName = args[1];
		final String type = args[2];
		
		final Configuration conf = HBaseConfiguration.create();
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

		final String columnFamily = "test";
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
		Thread threads[] = new Thread[numThreads];
		for (int i = 0; i < threads.length; i++) {
			threads[i] = new Thread() {
				public void run() {
					long bytes = 0;
					try {
						System.out.println("add rows");
						HTable table = new HTable(conf, tableName);
						if (type.equals("throughput")) {
							for (int i = 0; i < 1000 / numThreads; i++) {
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
							for (int i = 0; i < 1000 / numThreads; i++) {
								String rowKey = new Integer(i + 1000).toString();
								Put put = new Put(rowKey.getBytes());
								put.add(columnFamily.getBytes(), 
										new Integer(i + 1000).toString().getBytes(),
										new byte[1]);
								bytes++;
								table.put(put);
							}
						}
						table.close();

					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			};
			threads[i].start();
		}
		for (int i = 0; i < threads.length; i++)
			threads[i].join();
		end = System.currentTimeMillis();
		System.out.println("inserting takes: " + (((double) end - start) / 1000));

	}
}
