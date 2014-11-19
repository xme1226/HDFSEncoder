import java.io.*;
import org.javaswift.joss.client.factory.AccountConfig;
import org.javaswift.joss.model.Account;
import org.javaswift.joss.client.factory.AccountFactory;
import org.javaswift.joss.model.StoredObject;
import org.javaswift.joss.model.Container;
import java.util.*;
import org.javaswift.joss.client.factory.AuthenticationMethod;
import org.javaswift.joss.command.impl.identity.BasicAuthenticationCommandImpl;
import org.javaswift.joss.command.impl.identity.ExternalAuthenticationCommandImpl;
import org.javaswift.joss.command.impl.identity.KeystoneAuthenticationCommandImpl;
import org.javaswift.joss.command.impl.identity.TempAuthAuthenticationCommandImpl;
import org.javaswift.joss.model.Access;

import static org.javaswift.joss.client.factory.AuthenticationMethod.BASIC;
import static org.javaswift.joss.client.factory.AuthenticationMethod.KEYSTONE;
import static org.javaswift.joss.client.factory.AuthenticationMethod.TEMPAUTH;
import static org.javaswift.joss.client.factory.AuthenticationMethod.EXTERNAL;

public class Merge {
	public static void main(String[] args) throws Exception
	{
		String[] urls = args[0].split("/");
		String fileName = urls[3];
		String objName = merge(fileName,args[1]);
		uploadObject(args[0],objName);
	}

	private static void uploadObject(String args,String objName) throws IOException{
		AccountConfig config = new AccountConfig();
            	config.setUsername("admin");
            	config.setPassword("qwer1234");
            	config.setAuthUrl("http://172.16.100.168:5000/v2.0/tokens");
		config.setTenantName("admin");
            	config.setAuthenticationMethod(AuthenticationMethod.KEYSTONE);

		String[] urls = args.split("/");
		String contName = urls[2].substring(0,urls[3].indexOf("."));
		Account account = new AccountFactory(config).createAccount();	
		
		Container container = account.getContainer(contName);

		StoredObject object = container.getObject(objName);
    		object.uploadObject(new File("/home/hadoop/"+objName));
    		System.out.println("Public URL: "+object.getPublicURL());	
		File f = new File("/home/hadoop/"+objName);
                f.delete();
	}
	private static String merge(String args, String args2) throws IOException {
		String fName = args.substring(0,args.lastIndexOf("."));	
		String fileName = "mylist.txt";
		String movieName;
		BufferedWriter Writer = new BufferedWriter(new FileWriter("/home/hadoop/"+fileName));
		
		int n = Integer.parseInt(args2);
		for (int i=0;i<n;i++){
			movieName = fName+"_"+String.valueOf(i)+".flv";
			hdfsutil_sp.getInstance().copyToLocal("/user/hadoop/data/"+fName+"/encoded/"+movieName, "/home/hadoop/");		
			Writer.write("file '/home/hadoop/"+movieName+"'\n");
		}
		Writer.close();	
		converter_me.getInstance().performMerge("/home/hadoop/"+fileName,"/home/hadoop/"+fName+".flv");
		File f;
		for (int i=0;i<n;i++){
			movieName = fName+"_"+String.valueOf(i)+".flv";
			f = new File("/home/hadoop/"+movieName);
			f.delete();
		}
		f = new File("/home/hadoop/"+fileName);
		f.delete();
		
		return fName+".flv";
	}
}

