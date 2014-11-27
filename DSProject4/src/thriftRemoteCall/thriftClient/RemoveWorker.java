package thriftRemoteCall.thriftClient;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import thriftRemoteCall.thriftUtil.FileStore;
import thriftRemoteCall.thriftUtil.NodeID;
import thriftRemoteCall.thriftUtil.SystemException;

public class RemoveWorker {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		TTransport transport;
		TProtocol protocol = null;
		List<NodeID> fingerworker = null;
		try {
	
	 	transport = new TSocket("localhost", 9092);
			transport.open();

			protocol = new TBinaryProtocol(transport);
			FileStore.Client client = new FileStore.Client(protocol);
			client.remove();

			transport.close();
		
		} catch (SystemException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (TException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	public static String getSHAHash(String content)
	{
		String tobehashed = null;
		tobehashed = content;
		StringBuffer sbuff = null;
		sbuff = new StringBuffer();
		SystemException excep = null;
		BigInteger tempBig = null;
		try {
			MessageDigest md = MessageDigest.getInstance("SHA-256");
			md.update(tobehashed.getBytes());
			byte[] digest = md.digest();

			tempBig = new BigInteger(1,digest);

			sbuff = new StringBuffer();
			for (byte b : digest) {
				sbuff.append(String.format("%02x", b & 0xff));
			}
		} catch (NoSuchAlgorithmException e) {
			excep = new SystemException();
			excep.setMessage("Error while generating MD5 Hash Code.");			
		}
		return sbuff.toString();
	}
	public static void writeFingertable(List<NodeID> fingerworker,int port)
	{
		
		 
		File file = new File("test.txt"+port+"remove");
		try{
		// if file doesnt exists, then create it
		if (!file.exists()) {
			file.createNewFile();
		}

		FileWriter fw = new FileWriter(file.getAbsoluteFile());
		BufferedWriter bw = new BufferedWriter(fw);
		
		for (int i = 0; i < fingerworker.size(); i++) {
			bw.write(fingerworker.get(i).toString());
		}
		bw.close();
		}
		catch (Exception e) {
			// TODO: handle exception
		}
	}

}
