package com.xfyan.hive;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/**
 * 接受两个参数，返回第一列的第一个字符和第二列的最后一个字符的拼接 
 *
 */
public class UDFTest extends UDF {
	private Text text = new Text();
	
	public Text evaluate(Text str1,Text str2){
		if(str1 ==null || str2 == null){
			text.set("unvalid");
			return text;
		}
		text.set(String.valueOf(str1.toString().charAt(0)) + String.valueOf(str2.toString().charAt(str2.getLength() -1)));
		
		return text;
	}
}
