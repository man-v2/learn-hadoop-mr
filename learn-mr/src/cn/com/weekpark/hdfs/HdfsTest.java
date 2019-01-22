package cn.com.weekpark.hdfs;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

public class HdfsTest {
	
	private static final String HDFS_URI = "hdfs://master:50010";
	
	public static List<String> listDir(String[] path) throws IOException{
		List<String> result = new ArrayList<String>();
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(HDFS_URI), conf);
		
		Path[] paths = new Path[path.length];
		for(int i=0; i<paths.length; i++) {
			paths[i] = new Path(path[i]);
		}
		
		FileStatus[] status = fs.listStatus(paths);
		Path[] listPaths = FileUtil.stat2Paths(status);
		for(Path p : listPaths) {
			System.out.println(p);
		}
		return null;
	}

	public static void main(String[] args) {
		String[] path = {"/user/root/input"};
		try {
			HdfsTest.listDir(path);
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

}
