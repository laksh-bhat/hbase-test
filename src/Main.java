import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
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
	static public byte[] readFile(File file) throws IOException {
		FileInputStream in = new FileInputStream(file);
		byte bs[] = new byte[(int) file.length()];
		int readBytes = in.read(bs);
		assert(readBytes == bs.length);
		return bs;
	}
	
	static public void main(String args[]) throws IOException, InterruptedException {
		long start, end;
		
		if (args.length < 4) {
			System.err.println("argments: host table_name dir_name type");
			return;
		}
		
		String host = args[0];
		String tableName = args[1];
		String dirName = args[2];
		String type = args[3];

//		File dir = new File(dirName);
//		File[] files = dir.listFiles();
//		Arrays.sort(files, new Comparator<File>() {
//			@Override
//			public int compare(File f0, File f1) {
//				return f0.getName().compareTo(f1.getName());
//			}
//		});
		String keys[] = new String[20000];
		int fileSizes[] = new int[keys.length];
		long dataSize = 0;
		Random r = new Random();
		for (int i = 0; i < keys.length; i++) {
			keys[i] = "" + i;
			fileSizes[i] = 220000 + r.nextInt(30000);
			dataSize += fileSizes[i];
		}
		System.out.println("total data size: " + dataSize);
		int numRegions = (int) (dataSize / 4 / 1024 / 1024);
		int numFilesPerRegion = keys.length / numRegions;
		byte[][] splits = new byte[numRegions - 1][];
		for (int i = 0; i < splits.length; i++) {
			byte keyBytes[] = keys[(i + 1) * numFilesPerRegion].getBytes();
			splits[i] = new byte[keyBytes.length];
			System.arraycopy(keyBytes, 0, splits[i], 0, keyBytes.length);
		}

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
		
		if (type.equals("create")) {
			start = System.currentTimeMillis();
			try {
				if (admin.tableExists(tableName)) {
					admin.disableTable(tableName);
					admin.deleteTable(tableName);
					System.out.println("delete table " + tableName);
				}
	
				HTableDescriptor tableDesc = new HTableDescriptor(tableName);
				System.out.println("create a table with " + (splits.length + 1) + " regions");
				admin.createTable(tableDesc, splits);
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
		}
		else if (type.equals("write")) {
			// insert rows
			start = System.currentTimeMillis();
			try {
				System.out.println("add rows");
				HTable table = new HTable(conf, tableName);
				LinkedList<Put> list = new LinkedList<Put>();
				for (int i = 0; i < keys.length; i++) {
					byte[] value = new byte[fileSizes[i]];
					String rowKey = keys[i];
					Put put = new Put(rowKey.getBytes());
					put.add(columnFamily.getBytes(), 
							new Integer(i).toString().getBytes(),
							value);
					System.out.println("key size: " + rowKey.getBytes().length
							+ ", value size: " + value.length);
					table.put(put);
//					list.add(put);
				}
//				table.put(list);
				table.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
			end = System.currentTimeMillis();
			System.out.println("inserting all data takes: " + (((double) end - start) / 1000));
		}
		else if (type.equals("read")) {
			// search for rows
			start = System.currentTimeMillis();
			long bytes = 0;
			try {
				System.out.println("get rows");
				HTable table = new HTable(conf, tableName);
				keys = randomPermute(keys);
				for (int i = 0; i < keys.length; i++) {
					String rowKey = keys[i];
					Get get = new Get(rowKey.getBytes());
					Result res = table.get(get);
					bytes += res.value().length;
					System.out.println("row key: " + rowKey + ", value: " + res.value().length);
				}
				table.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
			end = System.currentTimeMillis();
			System.out.println("read " + bytes + " bytes: " + (((double) end - start) / 1000));
		}
		else if (type.equals("range")) {
			try {
				System.out.println("range query");
				HTable table = new HTable(conf, tableName);
				Scan scan = new Scan(keys[0].getBytes(),
						keys[keys.length / 16].getBytes());
				ResultScanner scanner = table.getScanner(scan);
				// The other approach is to use a foreach loop. Scanners are iterable!
				for (Result result = scanner.next(); result != null; result = scanner.next()) {
					NavigableMap<byte[], byte[]> map = result.getFamilyMap(columnFamily.getBytes());
					for (Map.Entry<byte[], byte[]> entry = map.pollFirstEntry(); entry != null; ) {
						System.out.println("Found row: " + Bytes.toString(result.getRow())
								+ " with value size: " + result.value().length);
						entry = map.pollFirstEntry();
					}
				}
				table.close();
			} catch(IOException e) {
				e.printStackTrace();
			}
		}

	}

	private static String[] randomPermute(String[] keys) {
		Random r = new Random();
		String[] permutedKeys = new String[keys.length];
		System.arraycopy(keys, 0, permutedKeys, 0, keys.length);
		for (int i = 0; i < permutedKeys.length; i++) {
			int randIdx = r.nextInt(keys.length);
			String tmp = permutedKeys[randIdx];
			permutedKeys[randIdx] = permutedKeys[i];
			permutedKeys[i] = tmp;
		}
		return permutedKeys;
	}
}
