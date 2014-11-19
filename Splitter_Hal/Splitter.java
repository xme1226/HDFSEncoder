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

public class Splitter {
	public static void main(String[] args) throws Exception
	{
		String fileName = downloadObject(args[0]);
		splitter(fileName,args[1]);
		File f = new File("/home/hadoop/"+fileName);
		f.delete();		
	}

	private static String downloadObject(String args) throws IOException{
		AccountConfig config = new AccountConfig();
            	config.setUsername("admin");
            	config.setPassword("qwer1234");
            	config.setAuthUrl("http://172.16.100.168:5000/v2.0/tokens");
		config.setTenantName("admin");
            	config.setAuthenticationMethod(AuthenticationMethod.KEYSTONE);

		String[] urls = args.split("/");
		String contName = urls[2].substring(0,urls[3].indexOf("."));
		String objName = urls[3];
		Account account = new AccountFactory(config).createAccount();	
		
		Collection<Container> containers = account.list();
    		for (Container currentContainer : containers) {
        		System.out.println(currentContainer.getName());
    		}	
		Container container = account.getContainer(contName);

		StoredObject object = container.getObject(objName);
    		object.downloadObject(new File("/home/hadoop/"+objName));
    		System.out.println("Public URL: "+object.getPublicURL());	
		return objName;
	}

	private static String splitter(String args, String args2) throws IOException {
		// TODO Auto-generated method stub
		
		String ffmpegPath = "ffmpeg";    //예) /work/ffmpeg
		String fOriginal = args;  //실시간으로 업로드되는 파일
		String duration = null;
		String[] dura;
		int num = Integer.parseInt(args2);      // 나누는 개
		int pos;
		int duraS; // 동영상 총 
//		num = 4;
		String[] cmdLine = new String[]{ffmpegPath,
				"-i",                           // 변환시킬 파일위치
				"/home/hadoop/"+fOriginal,
		};

		// 프로세스 속성을 관리하는 ProcessBuilder 생성.
		ProcessBuilder pb = new ProcessBuilder(cmdLine);
		pb.redirectErrorStream(true);
		Process p = null;
		try {
			// 프로세스 작업을 실행
			p = pb.start();
		} catch (Exception e) {
			e.printStackTrace();
			p.destroy();
			return null;
		}
		duration = exhaustInputStream(p.getInputStream());   // 자식 프로세스에서 발생되는 inputstrem를 소비시켜야합니다.
	    
		try {
			// p의 자식 프로세스의 작업이 완료될 동안 p를 대기시킴
			p.waitFor();
		} catch (InterruptedException e) {
			p.destroy();
		}
		dura = duration.split(":");
		pos = dura[2].indexOf(".");
		if(dura[2].substring(pos+1,pos+2) == "00"){
			dura[2] = dura[2].substring(0, pos);
			duraS = (Integer.parseInt(dura[0])*60 + Integer.parseInt(dura[1]))*60 + Integer.parseInt(dura[2])+ num-1;
		}
		else{
			dura[2] = dura[2].substring(0, pos);
			duraS = (Integer.parseInt(dura[0])*60 + Integer.parseInt(dura[1]))*60 + Integer.parseInt(dura[2])+ num;
		}

		p.destroy();

		cmdLine = makeFfmpegCommand(ffmpegPath,"/home/hadoop/"+fOriginal,duraS / num);
		
		ProcessBuilder pb2 = new ProcessBuilder(cmdLine);
		pb2.redirectErrorStream(true);
		Process pp = null;
		try {
			// 프로세스 작업을 실행
			pp = pb2.start();
		} catch (Exception e) {
			e.printStackTrace();
			pp.destroy();
			return null;
		}
		duration = exhaustInputStream(pp.getInputStream());   // 자식 프로세스에서 발생되는 inputstrem를 소비시켜야합니다.
	    
		try {
			pp.waitFor();
		} catch (InterruptedException e) {
			pp.destroy();
		}
		pp.destroy();
	
		/*---------------------------------------- 스플릿 끝-------------------------------------------*/

		String fName = fOriginal.substring(0,fOriginal.lastIndexOf("."));	
		hdfsutil_sp.getInstance().mkdir("/user/hadoop/data/"+fName+"/splitted/");
		hdfsutil_sp.getInstance().mkdir("/user/hadoop/data/"+fName+"/encoded/");
		hdfsutil_sp.getInstance().mkdir("/user/hadoop/data/"+fName+"/inputs/");
		
		for(int i=0; i< num; i++){
			makeInputFile(i,fName);
		}	
		

		return null;
	}
	private static void makeInputFile(int num, String fName){
		try{
			String fileName = "input" + String.valueOf(num) + ".txt"; 
			String mediaName = fName +"_"+ String.valueOf(num) + ".mp4";
			BufferedWriter Writer = new BufferedWriter(new FileWriter(fileName));
			Writer.write(mediaName);
			Writer.close();	
			hdfsutil_sp.getInstance().copyFromLocal(fileName,"/user/hadoop/data/" + fName + "/inputs/");	
			hdfsutil_sp.getInstance().copyFromLocal("/home/hadoop/"+mediaName,"/user/hadoop/data/"+fName+"/splitted/");
			File f = new File(fileName);
			f.delete();		
			f = new File("/home/hadoop/"+mediaName);
			f.delete();
		}
		catch(Exception e){
		}	
	}
	private static String[] makeFfmpegCommand(String ffmpegPath,
			String fOriginal, int duraEach) {
		String retok = fOriginal.substring(0,fOriginal.lastIndexOf("."));	
		String[] cmdLine =	new String[]{ffmpegPath,
				"-i", 
				fOriginal,
				"-acodec",
				"copy",
				"-f",
				"segment",
				"-segment_time",
				String.valueOf(duraEach),
				"-vcodec",
				"copy",
				"-reset_timestamps",
				"1",
				"-map",
				"0",
				retok+"_%d"+".mp4"};
		return cmdLine;

	}

	private static String exhaustInputStream(InputStream inputStream) {
		// TODO Auto-generated method stub
		// InputStream.read() 에서 블럭상태에 빠지기 때문에 따로 쓰레드를 구현하여 스트림을 소비한다
		String result = null;

		try {
			BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));
			String cmd = null;
			int pos = 0;
			while((cmd = br.readLine()) != null) { // 읽어들일 라인이 없을때까지 계속 반복
				if(cmd.matches(".*Duration:.*"))
				{
					pos = cmd.indexOf(",");
					result = cmd.substring(pos-11,pos);
				}
			}
			br.close();
		} catch(IOException e) {
			e.printStackTrace();
		}
		return result;
	}
}

