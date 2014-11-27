package thriftRemoteCall.thriftServer;


import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.server.TServer.Args;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;

import thriftRemoteCall.thriftServiceHandler.FileServiceHandler;
import thriftRemoteCall.thriftUtil.DHTNode;
import thriftRemoteCall.thriftUtil.DHTNode.Iface;
import thriftRemoteCall.thriftUtil.DHTNode.Processor;


public class FileServer {

	public static FileServiceHandler fileHandler;
	//public static FileStore.Processor<Iface> processor;
	public static DHTNode.Processor<Iface> processor;

	public static void StartsimpleServer(Processor<Iface> processor,int port) {

		try {

			TServerTransport serverTransport = new TServerSocket(port);
			TServer server = new TSimpleServer(new Args(serverTransport).processor(processor));

			System.out.println("Starting the File server...");
			server.serve();

		} catch (Exception e) {
			//e.printStackTrace();
			System.out.println("File server may running already..!!");
			System.exit(0);
		}
	}

	public static void main(String[] args) {
		int port;
		try{
			port = Integer.parseInt(args[0].trim());
			fileHandler = new FileServiceHandler(port);
			//processor = new FileStore.Processor<FileStore.Iface>(fileHandler);
			processor = new DHTNode.Processor<DHTNode.Iface>(fileHandler);
			//System.out.println("Port is : "+args[0]);
			StartsimpleServer(processor, port);
			
			//StartsimpleServer(new FileStore.Processor<FileServiceHandler>(new FileServiceHandler()),port);
		}catch(NumberFormatException e)
		{
			System.out.println("Port Number must be integer..!");
			System.exit(0);
		}catch (ArrayIndexOutOfBoundsException e) {
			System.out.println("Enter port number to run the server..!");
			System.exit(0);
		}

	}

}
