import java.io.*;

public class converter_me {
	
	private static converter_me instance = new converter_me();

	public static converter_me getInstance(){
		return instance;
	}
	/*public static void main(String[] args) 
	{
		System.out.println(args[0]);
		System.out.println(args[1]);
	 
		convert(args[0],args[1]);
	}
*/
	public void performMerge(String concatFile, String outputFile) throws IOException {
		String FFMPEG_MERGE_CMD = "ffmpeg -f concat -i " + concatFile + " -c copy " + outputFile;
		//FFMPEG_MERGE_CMD = "ffmpeg -f concat -i mylist.txt -c copy output.flv";
		Process pr = Runtime.getRuntime().exec(FFMPEG_MERGE_CMD);
		InputStream err = pr.getErrorStream();
		byte[] b = new byte[1024];
		int size = 0;
		while((size = err.read(b)) != -1){
		//	System.out.println("===========================Merge error ==================" + new String(b, 0, size));
		}
		InputStream is = pr.getInputStream();
		size = 0;
		while((size = is.read(b)) != -1){
		//	System.out.println("===========================Merge output ==================" + new String(b, 0, size));
		}
	}

	public String encodeFile(String inputFile, String outputFile) throws IOException {
		String FFMPEG_ENCODE_CMD = "ffmpeg -i " + inputFile + " -ar 44100 -ab 32 -s 500x300 -b 768k -r 24 -y -f flv " + outputFile;	

		Process pr = Runtime.getRuntime().exec(FFMPEG_ENCODE_CMD);
		InputStream err = pr.getErrorStream();
		byte[] b = new byte[1024];
		int size = 0;
		while((size = err.read(b)) != -1){
			System.out.println("===========================Merge error ==================" + new String(b, 0, size));
		}
		InputStream is = pr.getInputStream();
		size = 0;
		while((size = is.read(b)) != -1){
			System.out.println("===========================Merge output ==================" + new String(b, 0, size));
		}
		return outputFile;
	}

	public static String convert(String args, String args2) {
		
		/*String ffmpegPath = "ffmpeg";    //예) /work/ffmpeg
		String fOriginal = args;  //실시간으로 업로드되는 파일
		String fResult = args2;      // 인코딩하고 저장 할 파일위치

		String[] cmdLine = new String[]{ffmpegPath,
                "-i",                           // 변환시킬 파일위치
                fOriginal,      
                "-ar",
                "44100",                
                "-ab",
                "32",                      
                "-s",
                "500x300",     //화면 사이즈
                "-b",
                "768k",          //비트레이트
                "-r",
                "24",           //영상 프레임
                "-y",
                "-f",
                "flv",            // flv파일 형태로 출력
                fResult};  // 저장하는 위치입니다.

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
		exhaustInputStream(p.getInputStream());   // 자식 프로세스에서 발생되는 inputstrem를 소비시켜야합니다.

		 try {
		        // p의 자식 프로세스의 작업이 완료될 동안 p를 대기시킴
		        p.waitFor();
		 } catch (InterruptedException e) {
		         p.destroy();
		 }
		 
		// 정상 종료가 되지 않았을 경우
		 if (p.exitValue() != 0) {
		       System.out.println("변환 중 에러 발생");
		       return null;
		 }
		   // 변환을 하는 중 에러가 발생하여 파일의 크기가 0일 경우
		  if (fResult.length() == 0) {
		        System.out.println("변환된 파일의 사이즈가 0임");
		         return null;
		  }
		  p.destroy();
		*/ 
		return null;
	}

	private static void exhaustInputStream(InputStream inputStream) {
		// TODO Auto-generated method stub
	    // InputStream.read() 에서 블럭상태에 빠지기 때문에 따로 쓰레드를 구현하여 스트림을 소비한다
        try {
               BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));
               String cmd = null;
               while((cmd = br.readLine()) != null) { // 읽어들일 라인이 없을때까지 계속 반복
                  //System.out.println(cmd);
               }
               br.close();
            } catch(IOException e) {
               e.printStackTrace();
            }
	} 
}
