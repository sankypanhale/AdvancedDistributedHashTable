package thriftRemoteCall.thriftServer;


import org.apache.thrift.TException;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.server.TServer.Args;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;

import thriftRemoteCall.thriftServiceHandler.FileServiceHandler;
import thriftRemoteCall.thriftUtil.DHTNode;
import thriftRemoteCall.thriftUtil.DHTNode.Iface;
import thriftRemoteCall.thriftUtil.DHTNode.Processor;
import thriftRemoteCall.thriftUtil.SystemException;


public class FileServer {

	public static FileServiceHandler fileHandler;
	//public static FileStore.Processor<Iface> processor;
	public static DHTNode.Processor<Iface> processor;

	public static void StartsimpleServer(Processor<Iface> processor,int port) {

		try {

			TServerTransport serverTransport = new TServerSocket(port);
			//TServer server = new TSimpleServer(new Args(serverTransport).processor(processor));
			TServer server = new TThreadPoolServer(new TThreadPoolServer.Args(serverTransport).processor(processor));

			System.out.println("Starting the File server...");
			server.serve();

		} catch (Exception e) {
			//e.printStackTrace();
			System.out.println("File server may running already..!!");
			System.exit(0);
		}
	}

	public static void main(String[] args) {
		final int port;
		final int interval;
		try{
			port = Integer.parseInt(args[0].trim());
			interval = Integer.parseInt(args[1].trim());
			fileHandler = new FileServiceHandler(port);
			processor = new DHTNode.Processor<DHTNode.Iface>(fileHandler);
			

			//Code to create multi-threaded server
			Runnable simple = new Runnable() {
				int maxtime = 1500;
				@Override
				public void run() {
					// TODO Auto-generated method stub
					System.out.println("I am in RUN method");
					while(maxtime > 0)
					{    
						// Do your task
						simple(processor,interval);
						try{
							Thread.sleep(interval);
						} catch(Exception e){

						}
						maxtime--;
					}
					System.out.println("printing finger table at server side");
					try {
						System.out.println(fileHandler.getFingertable());
					} catch (SystemException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (TException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			};

			new Thread(simple).start();
			StartsimpleServer(processor, port);
		}catch(NumberFormatException e)
		{
			System.out.println("Port Number must be integer..!");
			System.exit(0);
		}catch (ArrayIndexOutOfBoundsException e) {
			System.out.println("Enter port number to run the server..!");
			System.exit(0);
		}

	}

	protected static void simple(Processor<Iface> processor, int interval) {
		// TODO Auto-generated method stub
		try {
			fileHandler.stabilize();
			fileHandler.fixFingers();
		} catch (SystemException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.out.println("Error occured in the thread");
		} catch (TException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.out.println("Error occured in the thread");
		}

	}

}
