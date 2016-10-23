package com.xfyan.hbase;

import java.util.Date;

public class Test {
	private static long Ip2Int(String ip) {
		
		long num = 0;
		String[] ips = ip.split("\\.");
		 try {
			if (ips.length == 4){  
			        num = Long.parseLong(ips[0], 10) * 256L * 256L * 256L + Long.parseLong(ips[1], 10) * 256L * 256L + Long.parseLong(ips[2], 10) * 256L + Long.parseLong(ips[3], 10);  
			        num = num >>> 0;  
			 }
		} catch (NumberFormatException e) {
			System.out.println(ip);
		}
		 
		 return num;
		
	}
	public static void main(String[] args) {
		Date date = new Date(System.currentTimeMillis());
		long hour = date.getHours();
		System.out.println(hour);
		
	}
}
