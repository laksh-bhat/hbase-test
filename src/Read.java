import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;

public class Read {
	static public void main(String args[]) {
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
		String columnFamily = "test";

		// insert rows
		start = System.currentTimeMillis();
		long bytes = 0;
		try {
			System.out.println("read rows");
			HTable table = new HTable(conf, tableName);
			Random r = new Random();
			if (type.equals("throughput")) {
				System.out.println("test throughput");
				for (int i = 0; i < 1000; i++) {
					String rowKey = new Integer(r.nextInt(1000)).toString();
					Get get = new Get(rowKey.getBytes());
//					get.addColumn(columnFamily.getBytes(), 
//							new Integer(i).toString().getBytes());
					Result res = table.get(get);
					if (res.isEmpty())
						System.out.println("the result is empty");
					else
						bytes += res.value().length;
				}
			}
			else if (type.equals("latency")) {
				System.out.println("test latency");
				for (int i = 1000; i < 2000; i++) {
					String rowKey = new Integer(r.nextInt(1000) + 1000).toString();
					Get get = new Get(rowKey.getBytes());
//					get.addColumn(columnFamily.getBytes(), 
//							new Integer(i).toString().getBytes());
					Result res = table.get(get);
					bytes += res.value().length;
				}
			}
			table.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		end = System.currentTimeMillis();
		System.out.println("reading " + bytes + "B takes: " + (((double) end - start) / 1000));

	}
}
