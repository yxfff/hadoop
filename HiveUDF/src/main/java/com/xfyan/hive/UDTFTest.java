package com.xfyan.hive;

import java.util.ArrayList;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

/**
 *继承GenericUDTF类 
 *实现initialize,process,close方法
 *initialize返回UDTF行信息（个数，类型）
 *process处理后通过forword方法把结果返回
 *最后调用close方法
 */
public class UDTFTest extends GenericUDTF{
	
	
	@Override
	public void close() throws HiveException {
		
	}

	@Override
	public void process(Object[] args) throws HiveException {
		String input = args[0].toString();
		String[] kvs = input.split(";");
		for(int i=0;i< kvs.length;i++){
			try {
				String[] result = kvs[i].split(":");
				forward(result);
			} catch (Exception e) {
				continue;
			}
		}
	}

	@Override
	public StructObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {
		if(args.length != 1){
			throw new UDFArgumentLengthException("ExplodeMap takes only one argument");
		}
		
		if(args[0].getCategory() != ObjectInspector.Category.PRIMITIVE){
			throw new UDFArgumentException("ExplodeMap takes string as a parameter");
		}
		
		ArrayList<String> fieldNames = new ArrayList<String>();
		ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();
		fieldNames.add("col1");
		fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
		fieldNames.add("col2");
		fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
		
		return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
	}
	
}
