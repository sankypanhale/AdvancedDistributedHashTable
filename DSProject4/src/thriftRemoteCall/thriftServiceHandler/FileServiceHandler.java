package thriftRemoteCall.thriftServiceHandler;

import java.beans.FeatureDescriptor;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.math.BigInteger;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.w3c.dom.NodeList;

import ch.qos.logback.core.pattern.parser.Node;

import thriftRemoteCall.thriftUtil.DHTNode;
import thriftRemoteCall.thriftUtil.NodeID;
import thriftRemoteCall.thriftUtil.SystemException;

import java.util.Date;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import javax.security.auth.kerberos.KerberosKey;


public class FileServiceHandler implements DHTNode.Iface{

	private NodeID meNode;
	private List<NodeID> fingertable;
	private NodeID mysucessor;
	private NodeID mypredecessor;


	private static BigInteger tempBig;
	public FileServiceHandler(int port) {

		//	fingertable = new ArrayList<NodeID>();
		meNode = new NodeID();
		meNode.port = port;

		try {
			meNode.ip = Inet4Address.getLocalHost().getHostAddress();

		} catch (UnknownHostException e) {
			System.out.println("Error while getting IP of machine..!");
			System.exit(0);
		}
		meNode.id = getSHAHash(meNode.ip+":"+port);
		
		System.out.println("Big integer valued to: "+tempBig);
		System.out.println("Ip is current machine is: "+meNode.ip+":"+port);
		System.out.println("Current Node Hash key is: "+meNode.id);
		//sucessor = new NodeID();
		 
		mypredecessor = null;
		mysucessor = this.meNode;
		init_fingertable();
		System.out.println("Finger table created and initialised");
	}


	@Override
	public NodeID findSucc(String key) throws SystemException, TException {

		TTransport transport = null;
		TProtocol protocol = null;
		//System.out.println("Hash key for owner:filename is: " + key);
		NodeID nodetoconnect = null;
		NodeID nodetoreturn = null;

		//BigInteger equivalent of key
		byte[] b = new BigInteger(key,16).toByteArray();
		BigInteger tempBig2 = new BigInteger(b);
		//		System.out.println("Biginterger for key is:"+ tempBig2);

		//	System.out.println("Findsucc called");

		SystemException excep = null;
		try{
			if(myCompare(key,meNode.id) == 0)
			{
				//client requests is satisfied at the requested node itself
				return meNode;
			}
			else if(myCompare(key,meNode.id) > 0)
			{
				//key is greater than meNode.id
				if(myCompare(key,fingertable.get(0).id) <= 0)
				{
					//key is less than first finger table entry
					//temp/go directly to successor function
					//no need to go further in the findSucc loop
					//nodetoconnect = meNode;

					// return sucessor of current node
					return this.getNodeSucc();
				}
			}
			// removed else: if no above condition then just go for it

			nodetoconnect = findPred(key);
			//System.out.println("I have found predesor : "+ nodetoconnect);

			if(nodetoconnect.getId().equalsIgnoreCase(meNode.getId()))
			{
				// if the sucessor is same node
				//no need to go for RPC
				//call local method
				return this.getNodeSucc();
			}

			//Sanket: new condition to handle equal to case in finger table
			if(nodetoconnect.getId().equalsIgnoreCase(key))
			{
				return nodetoconnect;
			}

			//call findSucc(recursive call) on nodetoconnect using RMI
			if(nodetoconnect != null)
			{
				transport = new TSocket(nodetoconnect.ip, nodetoconnect.port);
				transport.open();

				protocol = new TBinaryProtocol(transport);
				//FileStore.Client client = new FileStore.Client(protocol);
				DHTNode.Client client = new DHTNode.Client(protocol);
				//System.out.println("Calling sucessor of : "+nodetoconnect);
				nodetoreturn = client.getNodeSucc();
				transport.close();
				return nodetoreturn;
			}
			else
			{
				excep = new SystemException();
				excep.setMessage("Node corresponding to key not found..!!");
				throw excep;
			}
		}
		catch (SystemException e) {
			throw e;
		}
		catch (IndexOutOfBoundsException e) {
			excep = new SystemException();
			excep.setMessage("Finger table not initialised properly..!!");
			throw excep;
		}
	}




	@Override
	public NodeID findPred(String key) throws SystemException, TException {
		TTransport transport = null;
		TProtocol protocol = null;
		NodeID pred = null;
		NodeID next = null;
		NodeID nodetoreturn = null;
		int i;
		SystemException excep;
		boolean recurse = true;
		//	System.out.println("findPred called on "+this.meNode + "for the key: "+key);
		//System.out.println(key.length());
		if(myCompare(key,meNode.id) > 0)
			//if(key.myCompare(meNode.id) > 0 || key.myCompare(fingertable.get(0).getId()) <= 0)
		{
			//key is greater than meNode.id
			if(myCompare(key,fingertable.get(0).getId()) <= 0)
			{
				//key is less than or equal to first finger table entry
				//go directly to successor function
				//no need to go further in the findSucc loop
				//System.out.println("Special Case achived..!");
				// return current node
				return meNode;
			}
		}

		//Sanket: duplicate logic
		if(myCompare(key,fingertable.get(0).getId()) == 0)
		{
			return meNode;
		}


		if(myCompare(meNode.getId(),fingertable.get(0).getId()) > 0)
		{
			//condition is satisfied for the last node in the chord
			//first finger table entry will less than current node key

			//System.out.println("I am in new base case 1 ");

			//BigInteger equivalent of key so that perform arithmetic 
			byte[] b = new BigInteger(key,16).toByteArray();
			BigInteger keyBig = new BigInteger(b);
			//System.out.println("Biginterger for key is:"+ keyBig);


			b = new BigInteger(fingertable.get(0).getId(),16).toByteArray();
			keyBig = new BigInteger(b);
			//System.out.println("Biginterger for first finger table entry at 92 is:"+ keyBig);

			if((myCompare(key,fingertable.get(0).getId()) < 0) || (myCompare(key,meNode.getId()) > 0))
			{
				//first finger table entry is greater than the key
				//means that first entry will be successor of the key
				//i.e current node must be predecesssor
				//System.out.println("I am in new base case 2");
				return meNode;
			}
		}
		try
		{
			for(i=0; i < fingertable.size()-1; i++)
			{
				pred = fingertable.get(i);
				next = fingertable.get(i+1);
				/*if(i==253)
					System.out.println("Test done..!!");
				if(pred.getPort()== 9092)
				{
					System.out.println("current entry port: "+pred.getPort());
					System.out.println("next entry port: "+next.getPort());
				}*/
				if(pred != null && next != null)
				{
					if(myCompare(key,pred.getId()) == 0)
					{
						//System.out.println("Exact key found!!!");
						// if key matches to current node
						nodetoreturn = pred;
						//return pred;
						recurse = false;
						break;
					}
					if(myCompare(pred.getId(),next.getId()) > 0	)
					{
						//additional case if i entry is greater than i+1
						//in this case just return the first entry blindly
						if(myCompare(pred.getId(),key) <= 0 || myCompare(next.getId(),key) >=0)
						{	
							nodetoreturn = pred;
							recurse = false;
							break;
						}

					}
					else if(myCompare(key,pred.getId()) > 0)
					{
						if(myCompare(key,next.getId()) <= 0)
						{
							// if key is in between finger table entry i and i+1
							//System.out.println("Entry between i and i+1 found");
							nodetoreturn = pred;
							//return pred;
							break;
						}
					}
				}
				else if(next == null)
				{
					nodetoreturn = pred;
					break;
				}
			}
			//if(recurse)
			// go in only if nodeto return is not set
			if(nodetoreturn == null)
			{
				// this is to avoid problem when reverse entry comes at last
				i++; //i++;
				if(i == fingertable.size())
				{
					// if last record is reached in finger table return last node
					//System.out.println("Last entry is returned..!!");
					nodetoreturn = next;
					//return next;
				}
			}
		}catch(IndexOutOfBoundsException e)
		{
			excep = new SystemException();
			excep.setMessage("Finger table is not intialised properely..!!");
			throw excep;
		}
		// in other condition return null

		//Sanket: to avoid the infinite loop
		if(nodetoreturn.getId().equalsIgnoreCase(this.getMeNode().getId()))
		{
			//to avoid the infinite loop the returned node is same as current node
			//System.out.println("I am avoiding the infinite loop..!!");
			recurse = false;
		}
		if(recurse){
			if(nodetoreturn != null)
			{
				try{
					transport = new TSocket(nodetoreturn.ip, nodetoreturn.port);
					transport.open();

					protocol = new TBinaryProtocol(transport);
					//FileStore.Client client = new FileStore.Client(protocol);
					DHTNode.Client client = new DHTNode.Client(protocol);
					//System.out.println("Calling findPredecessor of : "+nodetoreturn);
					nodetoreturn = client.findPred(key);
					transport.close();
				}
				catch (TTransportException x)
				{
					//System.out.println("Error while looping in hops...!!");
					excep = new SystemException();
					excep.setMessage("Error while looping in hops...!!");
					throw excep;
					//System.exit(0);
				}
				catch (TException x) {
					x.printStackTrace();
				} 
				return nodetoreturn;
			}
			else
			{
				//System.out.println("Error condition..!!");
				excep = new SystemException();
				excep.setMessage("Error while looping in hops...!!");
				throw excep;
			}
		}
		else
			return nodetoreturn;
	}



	@Override
	public NodeID getNodeSucc() throws SystemException, TException {
		SystemException excep = null;
		if(mysucessor != null)
		{
			//return this.sucessor;
			//return sucessor;
			return this.fingertable.get(0);
		}
		else
		{
			excep = new SystemException();
			excep.setMessage("Node not found corresponding to key...!!");
			throw excep;	
		}
	}	

	public static String getSHAHash(String content)
	{
		String tobehashed = null;
		tobehashed = content;
		StringBuffer sbuff = null;
		sbuff = new StringBuffer();
		SystemException excep = null;
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


	@Override
	public void join(NodeID nodeId) throws SystemException, TException {
		// TODO Auto-generated method stub
		TTransport transport = null;
		TProtocol protocol = null;

		NodeID nodeentrytoadd = null;
		//set predecessor to null
		this.setMypredecessor(null);

		//set successor variable and first fingertable entry
		transport = new TSocket(nodeId.getIp(), nodeId.getPort());
		transport.open();
		protocol = new TBinaryProtocol(transport);
		DHTNode.Client client = new DHTNode.Client(protocol);
		nodeentrytoadd = client.findSucc(this.meNode.getId());
		transport.close();
		this.fingertable.set(0,nodeentrytoadd);
		this.setMysucessor(nodeentrytoadd);

		//notify the successor that I have entered the network
		transport = new TSocket(this.getMysucessor().getIp(), this.getMysucessor().getPort());
		transport.open();
		protocol = new TBinaryProtocol(transport);
		DHTNode.Client client2 = new DHTNode.Client(protocol);
		client2.notify(this.meNode);
		transport.close();
	}


	@Override
	public NodeID getNodePred() throws SystemException, TException {
		// TODO Auto-generated method stub
		return this.getMypredecessor();
	}


	@Override
	public void stabilize() throws SystemException, TException {
		// TODO Auto-generated method stub
		TTransport transport = null;
		TProtocol protocol = null;

		NodeID predOfSucc = null;
		boolean notifypredOfSucc = false;

		//Sanket2: Check if my successor is me 
		if(myCompare(this.meNode.getId(),this.getMysucessor().getId()) != 0)
		{

			//get the predecessor of successor
			transport = new TSocket(this.getMysucessor().getIp(), this.getMysucessor().getPort());
			transport.open();
			protocol = new TBinaryProtocol(transport);
			DHTNode.Client client = new DHTNode.Client(protocol);
			predOfSucc = client.getNodePred();
			transport.close();

		}
		//Sanket: needs to check if apache thrift framework returns null 
		if(predOfSucc == null)
		{
			// then notify the successor that I am here; set me as your predecessor
			//notify the successor that I have entered the network
			//Sanket2: Check if my successor is me 
			if(myCompare(this.meNode.getId(),this.getMysucessor().getId()) != 0)
			{
				transport = new TSocket(this.getMysucessor().getIp(), this.getMysucessor().getPort());
				transport.open();
				protocol = new TBinaryProtocol(transport);
				DHTNode.Client client2 = new DHTNode.Client(protocol);
				client2.notify(this.meNode);
				transport.close();
			}
		}
		else
		{
			//special condition when my successor is smaller than me
			if(myCompare(this.meNode.getId(),this.getMysucessor().getId()) > 0)
			{
				// predOfSucc must be greater than meNode and meNodeSucc OR..
				if(myCompare(this.meNode.getId(), predOfSucc.getId()) < 0 && 
						myCompare(this.getMysucessor().getId(), predOfSucc.getId()) < 0)
				{
					//predOfSucc = 30, meNode = 28, meNode.currentSucc = 4 
					// then set meNode's successor to predofSucc
					this.setMysucessor(predOfSucc);

					//and notify predOfSucc to update its predecessor
					notifypredOfSucc = true;
				}
				else if((myCompare(this.meNode.getId(), predOfSucc.getId()) > 0 && 
						myCompare(this.getMysucessor().getId(), predOfSucc.getId()) > 0))
				{
					//.. OR the predOfSucc must be smaller than meNode and meNodeSucc
					//predOfSucc = 2, meNode = 28, meNode.currentSucc = 4
					// then set meNode's successor to predofSucc
					this.setMysucessor(predOfSucc);

					//and notify predOfSucc to update its predecessor
					notifypredOfSucc = true;
				}
			}
			else
			{
				//here my successor is greater than me (normal case)

				//if predofSucc must be smaller than my current successor
				if(myCompare(this.getMysucessor().getId(), predOfSucc.getId()) > 0)
				{
					// and predofSucc must be greater than me
					if(myCompare(this.meNode.getId(), predOfSucc.getId()) < 0)
					{
						// this means that, predofSucc(n is notebook example)
						//belongs to meNode(Np) and meNode's successor(Ns)
						// then set meNode's successor to predofSucc
						this.setMysucessor(predOfSucc);

						//and notify predOfSucc to update its predecessor
						notifypredOfSucc = true;
					}
				}
			}
			if(notifypredOfSucc)
			{
				// notify predOfSucc to update its predecessor
				transport = new TSocket(predOfSucc.getIp(), predOfSucc.getPort());
				transport.open();
				protocol = new TBinaryProtocol(transport);
				DHTNode.Client client2 = new DHTNode.Client(protocol);
				client2.notify(this.meNode);
				transport.close();
			}
		}

	}


	@Override
	public void notify(NodeID nodeId) throws SystemException, TException {
		// TODO Auto-generated method stub

		// if my predecessor is null then set nodeID as my predecessor 
		if(this.getMypredecessor() == null)
		{
			this.setMypredecessor(nodeId);
		}
		else
		{
			// i need to check if nodeID falls between me and my current predecessor

			//special condition when my predecessor is greater than me
			if(myCompare(this.meNode.getId(),this.getMypredecessor().getId()) < 0)
			{
				//check that nodeID must be greater than the current predecessor and menode
				if(myCompare(this.getMypredecessor().getId(), nodeId.getId()) < 0 && 
						myCompare(this.getMeNode().getId(), nodeId.getId()) < 0)
				{
					//condition if 30 joins to network in between 28 and 4
					this.setMypredecessor(nodeId);
				}
				else if(myCompare(this.getMypredecessor().getId(), nodeId.getId()) > 0 && 
						myCompare(this.getMeNode().getId(), nodeId.getId()) > 0)
				{
					//check that nodeID must be smaller than the current predecessor and menode
					//condition if 1 joins to network in between 28 and 4
					this.setMypredecessor(nodeId);
				}
			}
			else
			{
				//my predecessor is smaller than me (normal case)

				//check that nodeID must be greater than the current predecessor
				if(myCompare(this.getMypredecessor().getId(), nodeId.getId()) < 0)
				{
					// and nodeID must be smaller than current node
					if(myCompare(this.getMeNode().getId(), nodeId.getId()) > 0)
					{
						// if this conditions are satisfied then it means that
						// newly joined node(nodeID) is in between this node 
						// and it's this node's current predecessor
						// hence update this node's predecessor
						this.setMypredecessor(nodeId);
					}
				}
			}
		}
	}


	@Override
	public void fixFingers() throws SystemException, TException {
		// TODO Auto-generated method stub
		int randomnumber;
		NodeID updatedEntry = null;
		BigInteger bigtwo = new BigInteger("2");
		BigInteger bigone = new BigInteger("1");
		BigInteger twopowervalue = null;
		BigInteger bignewkey = null;
		BigInteger big256 = null;
		BigInteger big256minusone = null;
		String key = null;
		//BigInteger equivalent of new node key
		byte[] b = new BigInteger(this.meNode.getId(),16).toByteArray();
		BigInteger tempBig2 = new BigInteger(b);
		
		//get the random number
		randomnumber = getmyRandomNumberGenerator();
		
		// find successor of randomnumberTH finger
		twopowervalue = bigtwo.pow(randomnumber);
		bignewkey = twopowervalue.add(tempBig2);

		//BUG FIX: Check if the generated key is greater than (2^256-1)
		//if yes then mod the value by 256
		big256 = bigtwo.pow(256);
		//(2^256 - 1)
		big256minusone = big256.subtract(bigone);
		if(bignewkey.compareTo(big256minusone) > 0)
		{
			bignewkey.mod(big256);
		}
		key = bignewkey.toString(16);
		updatedEntry = findSucc(key);
		// update it to correct entry
		this.fingertable.set(randomnumber, updatedEntry);
	}

	@Override
	public List<NodeID> getFingertable() throws SystemException, TException {
		// TODO Auto-generated method stub
		return this.fingertable;
	}

	public void init_fingertable()
	{
		int i;
		// Initialize the finger table and set all entries to null

		fingertable = new ArrayList<NodeID>();
		this.fingertable.add(0,this.meNode);

		for(i=1; i<256; i++)
		{
			this.fingertable.add(i,null);
		}
		
	}	
	public BigInteger getBigIntegerEquivalent(String key)
	{
		byte[] b = new BigInteger(key,16).toByteArray();
		BigInteger tempBig2 = new BigInteger(b);
		//System.out.println("Biginterger for newly joining node is:"+ tempBig2);
		return tempBig2;
	}
	public String getHexStringEquuivalent(BigInteger b)
	{
		String key;
		key = b.toString(16);
		return key;
	}
	public static String getSHAHash(String ip,String port)
	{
		String tobehashed = null;
		tobehashed = ip+":"+port;
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
		} catch (NoSuchAlgorithmException e) {
			excep = new SystemException();
			excep.setMessage("Error while generating SHA-256 Hash Code for new node.");	
		}
		return sbuff.toString();
	}
	public int myCompare(String thiskey,String comparewithkey){

		BigInteger thisbig = null;
		BigInteger comparewithbig = null;
		BigInteger subvalue = null;

		BigInteger maxvalue = null;
		BigInteger bigtwo = new BigInteger("2");
		BigInteger thismodvalue = null;
		maxvalue = bigtwo.pow(256);
		int returnvalue;
		/*if(thiskey.length() > 64)
			System.out.println("thiskey is greater than 64");
		if(comparewithkey.length() > 64)
			System.out.println("compare with greater than 64");*/

		thisbig = getBigIntegerEquivalent(thiskey);
		comparewithbig = getBigIntegerEquivalent(comparewithkey);

		if(thiskey.length()!=comparewithkey.length())
		{
			thisbig = getBigIntegerEquivalent(thiskey);
			comparewithbig = getBigIntegerEquivalent(comparewithkey);
			//thisbig = thisbig.mod(maxvalue);
			//comparewithbig = comparewithbig.mod(maxvalue);
			subvalue = comparewithbig.subtract(thisbig);
			//subvalue = thisbig.subtract(comparewithbig);
			/*if(subvalue.signum() > 0 )
			{
				thismodvalue = thisbig.mod(maxvalue);
				if(thisbig.compareTo(thismodvalue) == 0)
				{
					returnvalue = -1;
				}
				else
					returnvalue = 1;
			}
			if(thisbig.mod(maxvalue).compareTo(comparewithbig) < 0)
			{
				returnvalue = 1;
			}*/
			if(subvalue.signum() > 0)
			{
				returnvalue = -1;
				//returnvalue = 1;
			}
			else if(subvalue.signum() < 0)
			{
				returnvalue = 1;
				//returnvalue = -1;
			}
			else
				returnvalue = 0;
		}
		else
		{
			returnvalue = thiskey.compareToIgnoreCase(comparewithkey);
		}

		return returnvalue;
	}



	public NodeID getMeNode() {
		return meNode;
	}
	public void setMeNode(NodeID meNode) {
		this.meNode = meNode;
	}
	public NodeID getMysucessor() {
		return mysucessor;
	}
	public void setMysucessor(NodeID mysucessor) {
		this.mysucessor = mysucessor;
	}
	public NodeID getMypredecessor() {
		return mypredecessor;
	}
	public void setMypredecessor(NodeID mypredecessor) {
		this.mypredecessor = mypredecessor;
	}


	public int getmyRandomNumberGenerator()
	{
		int randomnum,min,max;
		min = 1;
		max = 255;
		Random randomobj = new Random();
		randomnum = randomobj.nextInt(max - min + 1) + min;
		System.out.println("Random Number generated is: "+randomnum);
		return randomnum;
	}

	/*public String ifkeygreaterthanmaxvalue(String key)
	{
		String newkey = null;
		BigInteger maxvalue = null;
		BigInteger bigtwo = new BigInteger("2");
		//maxvalue = bigtwo.pow(256);
		maxvalue = bigtwo.pow(256);
		BigInteger bigkey = getBigIntegerEquivalent(key);

/*		if(bigkey.compareTo(maxvalue) > 0)
		{
			bigkey = bigkey.mod(maxvalue);
		}
		bigkey = bigkey.add(maxvalue);
		newkey = getHexStringEquuivalent(bigkey);
		return newkey;
	}*/
}
