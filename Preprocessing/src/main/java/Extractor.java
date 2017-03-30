
import java.io.*;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class Extractor {
	public Extractor() {
	}

	public static void main(String[] args) throws IOException {
		//inputPath: the raw data of amazon review;
		//userIndexPath: the 
		String inputpath = args[0];
		String userIndexPath = args[1];
		String prodIndexPath = args[2];
		String outPutPath = args[3];

		IndexExtractor indexExtractor = new IndexExtractor();
		indexExtractor.extractIndex(inputpath, userIndexPath, prodIndexPath);
		//go through the inputPath to build index for userIndex and prodIndex and output to file

		IndexBuilder indexBuilder = new IndexBuilder();
		HashMap usermap = indexBuilder.buildUserIndex(userIndexPath);
		HashMap productmap = indexBuilder.buildProdIndex(prodIndexPath);
		//read the userIndex and ProdIndex we just output  again to build two hashmap
		//usermap:<UserId,  UserIndex>
		//productmap:<ProductId,    ProductIndex>
		Configuration conf = new Configuration();		

		conf.addResource(new Path("/HADOOP_HOME/conf/core-site.xml"));
		conf.addResource(new Path("/HADOOP_HOME/conf/hdfs-site.xml"));
		FileSystem fs = FileSystem.get(conf);

		BufferedReader reader=new BufferedReader(new InputStreamReader(fs.open(new Path(inputpath))));

		Path outPut=new Path(outPutPath);
		if ( fs.exists( outPut )) { fs.delete( outPut, true ); } 
		OutputStream os = fs.create( outPut);
		BufferedWriter writer = new BufferedWriter( new OutputStreamWriter( os, "UTF-8" ) );


		if(!reader.ready()) {
			System.out.print("Can not read the file");
		} else {

			//read the whole file again, read every 11 line, and get the product id , user id and score, usring product id to get product index
			//usring user id to get user index. and output to the output.txt in format: userIndex:prodIndex:score

			int count = 0;
			String review = "";
			int cnt=0;

			String templine;
			String[] splitlines;
			// add all 11 line together and split it by "/n", we get a string list, each element of this list is a line, the first line is product id and 
			// forth line is productId and seventh line is score
			while((templine = reader.readLine()) != null) {
				++count;
				review = review + templine + "\n";
				if(count == 11) {
					cnt++;
					splitlines = review.split("\n");
					writer.write((String)usermap.get(splitlines[3].substring(15)) + ":" + (String)productmap.get(splitlines[0].substring(18)) + ":" + splitlines[6].substring(14) + "\n");
					count = 0;
					review = "";
					splitlines = null;
					if(cnt%1000==0)
						System.out.println("Output Index "+cnt);
				}
			}

			if(count == 10) {
				splitlines = review.split("\n");
				writer.write((String)usermap.get(splitlines[3].substring(15)) + ":" + (String)productmap.get(splitlines[0].substring(18)) + ":" + splitlines[6].substring(14) + "\n");
			}
			//this is for last 10 line, because last line may dont have "/n", so we just need to read 10 line


			reader.close();
			writer.close();
			fs.close();
		}

	}
}
