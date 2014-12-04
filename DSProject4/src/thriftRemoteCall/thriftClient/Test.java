package thriftRemoteCall.thriftClient;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Inet4Address;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.swing.plaf.SliderUI;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import thriftRemoteCall.thriftServer.FileServer;
import thriftRemoteCall.thriftServiceHandler.FileServiceHandler;
import thriftRemoteCall.thriftUtil.DHTNode;
import thriftRemoteCall.thriftUtil.NodeID;
import thriftRemoteCall.thriftUtil.SystemException;

public class Test implements Runnable {
	public static int port;
	public static int interval;
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

		int totalNumofNodes;
		List<NodeID> nodes;
		TTransport transport;
		TProtocol protocol = null;
		List<NodeID> listofnodes = new ArrayList<NodeID>();
		NodeID rootNode = new NodeID();
		HashMap<Integer,List<Integer>> pythonfingermap = new HashMap<Integer,List<Integer>>();
		HashMap<Integer,Integer> pythonsucessor = new HashMap<Integer,Integer>();
		HashMap<Integer,Integer> pythonpred = new HashMap<Integer,Integer>();

		List<NodeID> fingerFromRemote = null;
		NodeID succFromRemote = null;
		NodeID predFromRemote = null;
		CompareValues fingervalues = null;
		CompareValues succvalues = null;
		CompareValues predvalues = null;
		try{
			//parse all the input parameters
			port = Integer.parseInt(args[0].trim());
			interval= Integer.parseInt(args[1].trim());
			totalNumofNodes = Integer.parseInt(args[2].trim());

			//get the Ip address
			
			rootNode.setIp(Inet4Address.getLocalHost().getHostAddress());
			rootNode.setPort(port);

			//loop to initialize the <port>,<fingertable> map
			for (int i = 0; i < totalNumofNodes; i++) {
				Integer tempport =  new Integer(port + i);
				List<Integer> tempfingertable = new ArrayList<Integer>();
				pythonfingermap.put(tempport, tempfingertable);

			}

			//run the python code
			pythonfingermap = runPythonCode(pythonfingermap);
			pythonsucessor = setPythonSucessor(pythonfingermap,pythonsucessor);
			pythonpred = setPythonPred(pythonsucessor,pythonpred);
			//System.out.println("SUcc "+ pythonsucessor);
			//System.out.println("Pred "+ pythonpred);

			//spawn all the results
			for (int i = 0; i < totalNumofNodes; i++) {
				NodeID currentNode = new NodeID();
				currentNode.setIp(Inet4Address.getLocalHost().getHostAddress());
				int tempport = port + i;
				currentNode.setPort(tempport);
				//add node to list of nodes
				listofnodes.add(currentNode);
				//thread to start the server
				Runnable serverrun = new Thread(new FileServer(tempport, interval));
				Thread mythread = new Thread(serverrun);
				mythread.start();
			}

			//System.out.println("_------------>printing NODE ZERO: "+listofnodes);
			//join the nodes
			//sleep for a while to let the server start
			Thread.sleep(15000);
			for (int k = 0; k < listofnodes.size(); k++) 
			{
				if(listofnodes.get(k).getPort() != rootNode.getPort())
				{
					System.out.println("Joining "+listofnodes.get(k).getPort()+" to "+rootNode.port);
					transport = new TSocket(listofnodes.get(k).getIp(), listofnodes.get(k).getPort());
					transport.open();
					protocol = new TBinaryProtocol(transport);
					DHTNode.Client client2 = new DHTNode.Client(protocol);
					client2.join(rootNode);
					transport.close();
				}
			}

			int maxtime = 500000;
			while(maxtime > 0)
			{
				Thread.sleep(950);
				CompareValues iterationFinger = new CompareValues();
				CompareValues iterationSuccesor = new CompareValues();
				CompareValues iterationPredecessor = new CompareValues();
				for (int j = 0; j < listofnodes.size(); j++) {
					transport = new TSocket(listofnodes.get(j).getIp(), listofnodes.get(j).getPort());
					transport.open();
					protocol = new TBinaryProtocol(transport);
					DHTNode.Client client2 = new DHTNode.Client(protocol);

					fingerFromRemote = client2.getFingertable();
					succFromRemote = client2.getNodeSucc();
					//System.out.println("I am getting predecessor of TEST "+listofnodes.get(j).getPort());
					predFromRemote = client2.getNodePred();
					transport.close();
					fingervalues = compareFingerTable(pythonfingermap,fingerFromRemote,listofnodes.get(j).getPort());
					succvalues = compareSuccessor(pythonsucessor,succFromRemote,listofnodes.get(j).getPort());
					predvalues = comparePredecessor(pythonpred,predFromRemote,listofnodes.get(j).getPort());

					iterationFinger = myadd(iterationFinger,fingervalues);
					iterationSuccesor = myadd(iterationSuccesor,succvalues);
					iterationPredecessor = myadd(iterationPredecessor,predvalues);
				}
				System.out.println("Fingertable [Correct]= "+ iterationFinger.correct + "[Incorrect]= "+iterationFinger.incorrect);
				System.out.println("Sucessor [Correct]= "+ iterationSuccesor.correct + "[Incorrect]= "+iterationSuccesor.incorrect);
				System.out.println("Predecessor [Correct]= "+ iterationPredecessor.correct + "[Incorrect]= "+iterationPredecessor.incorrect);
			}
		}catch(NumberFormatException e)
		{
			e.printStackTrace();
			System.out.println("Port Number,Interval,Number of Nodes must be integer..!");
			System.exit(0);
		}catch (ArrayIndexOutOfBoundsException e) {
			System.out.println("Enter port number to run the server..!");
			System.exit(0);
		}
		catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}catch (SystemException e) {
			// TODO: handle exception
		}catch (TException e)
		{
			e.printStackTrace();
			System.out.println("Error while calling remote server");
			System.exit(0);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
	}

	private static CompareValues myadd(CompareValues finalcount,CompareValues currentvalue) {
		// TODO Auto-generated method stub
		finalcount.setCorrect(finalcount.getCorrect() + currentvalue.getCorrect());
		finalcount.setIncorrect(finalcount.getIncorrect()+ currentvalue.getIncorrect());
		return finalcount;
	}

	private static HashMap<Integer, Integer> setPythonPred(
			HashMap<Integer, Integer> pythonsucessor,HashMap<Integer, Integer> pythonpred) {
		// TODO Auto-generated method stub
		try{
			for (Entry<Integer, Integer> entry : pythonsucessor.entrySet())
			{
				if(entry.getKey() != 0)
				{
					pythonpred.put(entry.getValue(),entry.getKey());
				}
				//System.out.println(entry.getKey() + "/" + entry.getValue());
			}
		}catch (IndexOutOfBoundsException e) {
			System.out.println("Please check nodes.txt");
			System.exit(0);
		}
		return pythonpred;
	}
	private static HashMap<Integer,Integer> setPythonSucessor(
			HashMap<Integer, List<Integer>> pythonfingermap,HashMap<Integer, Integer> pythonsucessor) {
		// TODO Auto-generated method stub

		try{
			for (Entry<Integer, List<Integer>> entry : pythonfingermap.entrySet())
			{
				if(entry.getKey() != 0)
				{
					pythonsucessor.put(entry.getKey(), entry.getValue().get(0));
				}
				//System.out.println(entry.getKey() + "/" + entry.getValue());
			}
		}catch (IndexOutOfBoundsException e) {
			System.out.println("Please check nodes.txt");
			System.exit(0);
		}
		return pythonsucessor;
	}

	private static CompareValues compareFingerTable(HashMap<Integer, List<Integer>> pythonfingermap,List<NodeID> fingerFromRemote,int port) 
	{
		// TODO Auto-generated method stub
		CompareValues fingervalues = new CompareValues();
		Integer tempint = new Integer(port);

		List<Integer> localfinger = pythonfingermap.get(tempint);
		for (int i = 0; i < localfinger.size(); i++) {
			if(fingerFromRemote.get(i).getPort() != -1)
			{
				if(localfinger.get(i).intValue() != fingerFromRemote.get(i).getPort())
					fingervalues.incorrect++;
				else
					fingervalues.correct++;
			}
		}
		return fingervalues;
	}

	private static CompareValues comparePredecessor(HashMap<Integer, Integer> pythonpred, NodeID predFromRemote, int port) {
		// TODO Auto-generated method stub
		CompareValues predvalues = new CompareValues();
		Integer tempint = new Integer(port);
		Integer pythonSucc = pythonpred.get(tempint);
		if(predFromRemote.getPort() == pythonSucc.intValue())
		{
			predvalues.correct++;
		}
		else
		{
			predvalues.incorrect++;
		}
		return predvalues;
	}
	private static CompareValues compareSuccessor(HashMap<Integer, Integer> pythonsucessor, NodeID succFromRemote, int port) {
		// TODO Auto-generated method stub
		CompareValues succvalues = new CompareValues();
		Integer tempint = new Integer(port);
		Integer pythonSucc = pythonsucessor.get(tempint);
		if(succFromRemote.getPort() == pythonSucc.intValue())
		{
			succvalues.correct++;
		}
		else
		{
			succvalues.incorrect++;
		}
		return succvalues;
	}
	@Override
	public void run() {
		// TODO Auto-generated method stub
		System.out.println("I am in RUN of Test");
		FileServer fserver = null;
		fserver = new FileServer(port,interval);
		fserver.mainMethod();
	}

	private static HashMap<Integer, List<Integer>> runPythonCode(HashMap<Integer, List<Integer>> pythonfingermap)
	{
		String readPythonOutput = null;
		String[] splitscreen = null;
		int key = 0;
		//code to run python code
		try{
			Process p = Runtime.getRuntime().exec("./cmp_fingertables nodes.txt");
			BufferedReader stdInput = new BufferedReader(new InputStreamReader(p.getInputStream()));
			// read the output from the command
			System.out.println("Here is the standard output of the command:\n");
			while ((readPythonOutput = stdInput.readLine()) != null) {
				//System.out.println(readPythonOutput);
				splitscreen = readPythonOutput.split(" ");
				if(splitscreen.length > 0)
				{
					if(readPythonOutput.contains("Fingertable"))
					{
						//System.out.println("I got new finger table entry: "+splitscreen[3]);
						key = Integer.parseInt(splitscreen[3]);
					}
					else
					{
						List<Integer> templist = pythonfingermap.get(key);
						//System.out.println(readPythonOutput);
						if(readPythonOutput.length() > 0 && readPythonOutput != null)
							templist.add(Integer.parseInt(readPythonOutput.trim()));
						pythonfingermap.put(key, templist);
					}
				}
			}
			//System.out.println("Printing hashmap");
			//System.out.println(pythonfinger);
		}catch (IOException e) {
			// TODO Auto-generated catch block
			System.out.println("Error while running python code");
		}
		return pythonfingermap;
	}
}
