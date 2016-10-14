import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.GzipCodec;

import com.hadoop.compression.lzo.LzopCodec;


public class Code_Change {

	public static void main(String[] args) throws IOException {
		Configuration conf=new Configuration();
		Path src=new Path("file:///mnt/data/test.gz");
		FileSystem localFS=src.getFileSystem(conf);
		
		Path dst=new Path("hdfs:///home/fxx/test.lzo");
		FileSystem hdfsFS=dst.getFileSystem(conf);
		
		GzipCodec gzipCodec=new GzipCodec();
		gzipCodec.setConf(conf);
		InputStream gzipis=gzipCodec.createInputStream(localFS.open(src));
		
		LzopCodec lzopCodec =new LzopCodec();
		lzopCodec.setConf(conf);
		OutputStream lzopos=lzopCodec.createOutputStream(hdfsFS.create(dst));
		
		int length=1024;
		byte[] buffer=new byte[length];
		int size=0;
		try{
			while((size=gzipis.read(buffer, 0, length))>0){
				lzopos.write(buffer, 0, size);
			}
		}finally{
			gzipis.close();
			lzopos.close();
		}

	}

}
