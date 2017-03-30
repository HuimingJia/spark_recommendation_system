
import java.io.*;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class IndexExtractor {
	public IndexExtractor() {
	}

	public void extractIndex(String inputPath, String userIndexPath, String prodIndexPath) throws IOException {
		//this function is to go throught the raw data first time and build userIndex and productIndex and output to userIndex.txt and productIndex.txt
		HashSet UserSet = new HashSet();
		HashSet ProductSet = new HashSet();
		Configuration conf = new Configuration();


		conf.addResource(new Path("/HADOOP_HOME/conf/core-site.xml"));
		conf.addResource(new Path("/HADOOP_HOME/conf/hdfs-site.xml"));
		FileSystem fs = FileSystem.get(conf);

		BufferedReader reader=new BufferedReader(new InputStreamReader(fs.open(new Path(inputPath))));

		Path userIndex=new Path(userIndexPath);
		if ( fs.exists( userIndex )) { fs.delete( userIndex, true ); } 
		OutputStream os = fs.create( userIndex);
		BufferedWriter userIndexWriter = new BufferedWriter( new OutputStreamWriter( os, "UTF-8" ) );

		Path prodIndex=new Path(prodIndexPath);
		if ( fs.exists( prodIndex )) { fs.delete( prodIndex, true ); } 
		OutputStream os1 = fs.create( prodIndex);
		BufferedWriter prodIndexWriter = new BufferedWriter( new OutputStreamWriter( os1, "UTF-8" ) );

		if(!reader.ready()) {
			System.out.print("Can not read the file");
		} else {
			int count = 0;
			int cnt=0;
			String review = "";

			String templine;
			String[] splitlines;

			//read the file every 11 lines, put every 11 lines together to a string and split by "/n"
			//we will get a list of stirngs by statement:"splitlines = review.split("\n")", each element is a line
			//the first line is productId, the forth line is UserId, so I add it into hashset
			while((templine = reader.readLine()) != null) {
				++count;
				review = review + templine + "\n";
				if(count == 11) {
					cnt++;
					splitlines = review.split("\n");
					ProductSet.add(splitlines[0].substring(18));
					UserSet.add(splitlines[3].substring(15));
					count = 0;
					review = "";
					splitlines = null;
					if(cnt%1000==0)
						System.out.println("Index Extractor Building Hash Set: "+cnt);
				}
			}

			if(count == 10) {
				splitlines = review.split("\n");
				UserSet.add(splitlines[0].substring(18));
				ProductSet.add(splitlines[3].substring(15));
			}

			//Iterator for output
			Iterator iterator = UserSet.iterator();

			for(int usercount = 0; iterator.hasNext(); ++usercount) {
				userIndexWriter.write((String)iterator.next() + ":" + usercount + "\n");
				iterator.remove();
				if(usercount%1000==0)
					System.out.println("User Index "+usercount);
				if(usercount%1000000==0)
					userIndexWriter.flush();
			}
			UserSet.clear();
			
			iterator = ProductSet.iterator();

			for(int productcount = 0; iterator.hasNext(); ++productcount) {
				prodIndexWriter.write((String)iterator.next() + ":" + productcount + "\n");
				iterator.remove();
				if(productcount%1000==0)
					System.out.println("Product Index "+productcount);
				if(productcount%1000000==0)
					prodIndexWriter.flush();
			}

			ProductSet.clear();
			reader.close();
			userIndexWriter.close();
			prodIndexWriter.close();
			fs.close();
		}

	}
}
