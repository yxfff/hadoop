package cn.chinahadoop.filereverse;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Properties;
import java.util.Scanner;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args ) throws FileNotFoundException, UnsupportedEncodingException
    {
        File file=new File("test.txt");
        FileInputStream fis=new FileInputStream(file);
        Scanner in=new Scanner(fis,"GBK");
		ArrayList<String> lists=new ArrayList<String>();
		String line=null;
		while(in.hasNextLine()){
			line=in.nextLine();	
			lists.add(line);
		}
		in.close();
		Object[] strs= lists.toArray();
		
	
	    PrintWriter out=new PrintWriter("test2.txt","UTF-8");
		for(int j=strs.length-1;j>=0;j--){
			out.println(strs[j]);
		}
		out.close();
		
    }
}
