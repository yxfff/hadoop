package cn.chinahadoop.fileLoad;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Hello world!
 *
 */
public class App 
{
	public static Properties prop=new Properties();
	
	
	
    public static void main( String[] args )
    {
    	ScheduledExecutorService service=Executors.newSingleThreadScheduledExecutor();
    	service.scheduleAtFixedRate(new myThread(), 0, 1000, TimeUnit.MILLISECONDS);
    }
}
class myThread implements Runnable
{

	static Map<String,String> map;
	public void run() {
		try {
			App.prop.load(new FileInputStream("config.properties"));
			if(map==null){
				readConfig();
			}else{
				changedConfig();
			}
			
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private void changedConfig() {
		Set<String> keys=App.prop.stringPropertyNames();
	    for(String key:keys){
	    	String new_value=App.prop.getProperty(key);
	    	String old_value=map.get(key);
	    	if(!new_value.equals(old_value)){
	    		map.put(key, new_value);
	    		System.out.println(key+" is changed to "+new_value);
	    	}
	    	
	    }
		
	}

	private void readConfig() {
		map=new HashMap<String,String>();
	    Set<String> keys=App.prop.stringPropertyNames();
	    for(String key:keys){
	    	String value=App.prop.getProperty(key);
	    	map.put(key, value);
	    	System.out.println(key+" is "+value);
	    }
		
	}
	
}