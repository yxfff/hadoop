package com.xfyan.hive;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;

/**
 *作用：把某一列的值用指定分隔符连接起来
 *实现UDAFEvaluator接口，实现init,iterater,merge,terminatePartial,terminate方法
 */
@SuppressWarnings("deprecation")
public class UDAFTest extends UDAF{
	public static class ConcatUDAFEvaluator implements UDAFEvaluator{
		String line ="";
		public void init() {
			line = "";
		}
		
		
		public boolean iterater(String value,String separator){
			if(value != null || separator !=null){
				line += value+separator;
				
			}
			
			line += "";
			return true;
		}
		
		public String terminate(){
			return line;
		}
		public String terminatePartial(){
			return line;
		}
		public boolean merge(String another){
			return iterater(line,another);
		}
		
	}
	
	
}
