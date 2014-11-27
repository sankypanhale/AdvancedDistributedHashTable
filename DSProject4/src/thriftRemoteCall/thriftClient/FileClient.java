package thriftRemoteCall.thriftClient;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TJSONProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TIOStreamTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;


import thriftRemoteCall.thriftUtil.FileStore;
import thriftRemoteCall.thriftUtil.RFile;
import thriftRemoteCall.thriftUtil.NodeID;
import thriftRemoteCall.thriftUtil.RFileMetadata;
import thriftRemoteCall.thriftUtil.SystemException;
public class FileClient {

	public static void main(String[] args) {

		TTransport transport;
		TProtocol protocol = null;
		TIOStreamTransport transportConsole =null;
		TJSONProtocol protocolConsole = null;

		RFile rfileobject = null;
		RFile rfileobject2;


		FileReadHandlerClient filehandler = null;
		String operation = null;
		String filename = null;
		String server = null;
		String owner = null;
		int port;

		String operationkey = null;
		NodeID nodetoconnect = null;

		try {	
			if(!(args.length == 6 || args.length == 8))
			{
				System.out.println("Invalid Parameters...!!");
				System.out.println("Usage: ./client <servername> <port>"
						+ "--operation <operation> --filename <filename> --user <username>");
				return;
			}
			for(int i=2; i < args.length; i++)
			{
				if((args[i].trim()).equals("--operation"))
				{
					operation = args[i+1].trim().toLowerCase();
				}
				if((args[i].trim()).equals("--filename"))
				{
					filename = args[i+1].trim();
				}
				if((args[i].trim()).equals("--user"))
				{
					owner = args[i+1].trim().toLowerCase();
				}
			}

			RFileMetadata rmetaobject = new RFileMetadata();


			server = args[0];
			port = Integer.parseInt(args[1].trim());
			transport = new TSocket(server, port);
			transport.open();

			protocol = new TBinaryProtocol(transport);
			FileStore.Client client = new FileStore.Client(protocol);


			transportConsole = new TIOStreamTransport(System.out);
			protocolConsole = new TJSONProtocol(transportConsole);


			operationkey = getSHAHash(owner,filename);
			//System.out.println("Key for file is: "+operationkey);
			nodetoconnect = client.findSucc(operationkey);
			//nodetoconnect = client.findSucc("5d730683aeb0effe1e2ae2ba1517ddaf09ee5dc725587cbffc162378f199e4d6");
			transport.close();

			/*nodetoconnect = new NodeID();
			nodetoconnect.ip = "127.0.1.1";
			nodetoconnect.port = 9090;*/
			
			//System.out.println("request should be processed by: "+nodetoconnect.ip+":"+nodetoconnect.port);

			transport = new TSocket(nodetoconnect.getIp(), nodetoconnect.getPort());
			transport.open();

			protocol = new TBinaryProtocol(transport);
			FileStore.Client client2 = new FileStore.Client(protocol);

			if(operation.equals("write"))
			{
				//write
				rfileobject = new RFile();
				rmetaobject.setFilename(filename);
				rmetaobject.setOwner(owner);
				rfileobject.setMeta(rmetaobject);
				filehandler = new FileReadHandlerClient(filename);
				rfileobject.content = filehandler.readClientFile();
				//rfileobject.content = rfileobject.getMeta().getFilename();
				client2.writeFile(rfileobject);
			}
			else if(operation.equals("read"))
			{
				//read
				rfileobject2 = client2.readFile(filename, owner);
				rfileobject2.write(protocolConsole);
			}
			else if(operation.equals("delete"))
			{	
				//delete
				client2.deleteFile(filename,owner);			
			}
			else {
				System.out.println("Invalid operation...!!");
			}
			transport.close();
		}catch (SystemException sysexception)
		{
			try {
				sysexception.write(protocolConsole);
			} catch (TException e) {
				System.out.println("Error while printing exception");
				e.printStackTrace();
			}
		}
		catch (TTransportException x)
		{
			if(x != null)
			{
				System.out.println(x.getMessage());
			}
			else{
				System.out.println("Invalid Server Address...!!");}
			System.exit(0);
		}
		catch (TException x) {
			x.printStackTrace();
		} 
		catch(NumberFormatException e)
		{
			System.out.println("Port Number must be integer..!");
			System.exit(0);
		}catch (ArrayIndexOutOfBoundsException e) {
			System.out.println("Invalid Input Parameters..!");
			System.exit(0);
		}
		catch(NullPointerException e)
		{
			System.out.println("Invalid Input Parameters..!!");
			System.exit(0);
		}
	}
	public static String getSHAHash(String owner,String filename)
	{
		String tobehashed = null;
		String trimfilename = null;

		//System.out.println("File name is" + filename);
		String[] splitvalues = filename.split("/");
		trimfilename = splitvalues[splitvalues.length-1];
		//System.out.println("trimmed name is: "+ trimfilename);
		//tobehashed = owner+":"+filename;
		tobehashed = owner+":"+trimfilename;
		StringBuffer sbuff = null;
		sbuff = new StringBuffer();
		SystemException excep = null;
		try {
			MessageDigest md = MessageDigest.getInstance("SHA-256");
			md.update(tobehashed.getBytes());
			byte[] digest = md.digest();
			sbuff = new StringBuffer();
			for (byte b : digest) {
				sbuff.append(String.format("%02x", b & 0xff));
			}
			//System.out.println("original:" + filecontent);
			//System.out.println("digested(hex):" + sbuff.toString());

		} catch (NoSuchAlgorithmException e) {
			excep = new SystemException();
			excep.setMessage("Error while generating SHA-256 Hash Code.");	
		}
		return sbuff.toString();
	}
}
