import java.io.*;
import java.util.Arrays;

public class TestProcess{


	public static void main(String [] args) throws Exception{

		String[] newargs = {"ls", "-l"};
		ProcessBuilder processBuilder = new ProcessBuilder(newargs);
		Process process = processBuilder.start();

	}

}