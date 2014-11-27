package thriftRemoteCall.thriftClient;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

public class FileReadHandlerClient {

	private BufferedReader reader;
	public FileReadHandlerClient(String filename) {
		try {
			reader = new BufferedReader(new FileReader(filename));
		} catch (FileNotFoundException e) {
			System.out.println("Error : File not found on client machine..!");
			System.exit(0);
		}
	}
	public String readClientFile()
	{
		StringBuffer sbuff = null;
		String line = null;
		try {
			
			sbuff = new StringBuffer();
			while(true)
			{
				line = reader.readLine();
				if(line != null)
					sbuff.append(line+"\n");
				else 
					break;
			}
			reader.close();
		}  catch (IOException e){
			System.out.println("Error in reading file at client..!");
			System.exit(0);
		} finally{
			try {
				reader.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return sbuff.toString();
	}
}
