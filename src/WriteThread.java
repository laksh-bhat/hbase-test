import java.io.IOException;

import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;

public class WriteThread implements Runnable {
    int thread_id;
    //constructor                                                                                                             
    int numIter;
    static HTable table;
    static String type;
    static String columnFamily;
    long bytes;
    WriteThread(int id)
    {
        this.thread_id =id;
    }

    //Run method                                                                                                                
    public void run()
    {
	bytes = 0;
        //Create write object                                                                                                 
	// insert rows        
	int s = thread_id * numIter;
	int e = s + numIter;
	if (type.equals("throughput")) {
	    for (int i = s; i < e; i++) {
		String rowKey = new Integer(i).toString();
		Put put = new Put(rowKey.getBytes());
		put.add(columnFamily.getBytes(),
			new Integer(i).toString().getBytes(),
			new byte[1024 * 1024]);
		bytes += 1024 * 1024;
		try{
		    table.put(put);
		    
		} catch (IOException e1) {
		    e1.printStackTrace();
		}
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
		try{
                    table.put(put);

                } catch (IOException e2) {
                    e2.printStackTrace();
                }
	    }
	}
	  
        System.out.println("Thread"+this.thread_id);

    }
    static public void main(String args[]) throws InterruptedException {
	long start, end;
	
	if (args.length < 4) {
	    System.out.println("arguments: host table_name type");
	    System.out.println("type: throughput, latency");
	    return;
	}
	
	String host = args[0];
	String tableName = args[1];
	type = args[2];
	
	int numThreads = Integer.parseInt(args[3]);

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

	columnFamily = "test";
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

	Thread[] threads = new Thread[numThreads];
	WriteThread write_threads[] = new WriteThread[numThreads];
	
	start = System.currentTimeMillis();
	long totalbytes = 0;
	try {

	    System.out.println("add rows");
	    table = new HTable(conf, tableName);	    
	    for(int i=0;i<numThreads;i++){
		write_threads[i]=new WriteThread(i);
		write_threads[i].numIter = 1000/numThreads;
		threads[i]=new Thread(write_threads[i]);
		
	    }

	    for(int i=0;i<numThreads;i++)
		threads[i].start();
	
	    table.close();
	    
	} catch (IOException e) {
	    e.printStackTrace();
	}
	//Waiting for the threads to end
	for ( int i=0; i<numThreads; i++ )
	    {
		try
		    {  
			threads[i].join();
			//Adding the count from all the threads
			totalbytes=totalbytes+write_threads[i].bytes;
		    }
		catch (InterruptedException e)
		    {
			System.out.println("Thread interrupted.  Exception: " + e.toString() +" Message: " + e.getMessage());
			return;
		    }
	    }
	//Printing #Heads #Threads
	end = System.currentTimeMillis();
	System.out.println("inserting " + totalbytes + "B takes: " + (((double) end - start) / 1000));

    }
}
