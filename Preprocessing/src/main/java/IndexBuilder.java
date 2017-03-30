
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class IndexBuilder {
	public IndexBuilder() {
	}

	public HashMap buildUserIndex(String userIndexPath) throws IOException {
		//this function is to go throught the userIndex.txt to build a dictionary
		HashMap usermap = new HashMap();
		Configuration conf = new Configuration();


		conf.addResource(new Path("/HADOOP_HOME/conf/core-site.xml"));
		conf.addResource(new Path("/HADOOP_HOME/conf/hdfs-site.xml"));
		FileSystem fs = FileSystem.get(conf);

		BufferedReader reader=new BufferedReader(new InputStreamReader(fs.open(new Path(userIndexPath))));

		if(!reader.ready()) {
			System.out.print("Can not read the file");
		} else {
			while(true) {
				String templine;
				if((templine = reader.readLine()) == null) {
					reader.close();
					break;
				}

				String[] kvPair = templine.split(":");
				usermap.put(kvPair[0], kvPair[1]);
			}
		}
		reader.close();
		fs.close();
		return usermap;
	}

	public HashMap buildProdIndex(String prodIndexPath) throws IOException {
		//this function is to go throught the productIndex.txt to build a dictionary
		HashMap productmap = new HashMap();
		Configuration conf = new Configuration();


		conf.addResource(new Path("/HADOOP_HOME/conf/core-site.xml"));
		conf.addResource(new Path("/HADOOP_HOME/conf/hdfs-site.xml"));
		FileSystem fs = FileSystem.get(conf);

		BufferedReader reader=new BufferedReader(new InputStreamReader(fs.open(new Path(prodIndexPath))));

		if(!reader.ready()) {
			System.out.print("Can not read the file");
		} else {
			while(true) {
				String templine;
				if((templine = reader.readLine()) == null) {
					reader.close();
					break;
				}

				String[] kvPair = templine.split(":");
				productmap.put(kvPair[0], kvPair[1]);
			}
		}
		reader.close();
		fs.close();
		return productmap;
	}
}
