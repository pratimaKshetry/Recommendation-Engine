/* Original Author: Pratima Kshetry**********************/

package pkProductProfile;

import java.io.IOException;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class AmazonProductMR extends Configured implements Tool {

	public static class AMapper extends Mapper<Object, Text, Text, Text>
	{
		public void map(Text key, Text value, Context ctxt) throws IOException, InterruptedException
		{
			try
			{
				ctxt.write(key, value);
			}
			catch (Exception ex)
			{
				ex.printStackTrace();
				System.out.println("Error:"+ex.getMessage());
			}
			
		}
		
	}
	
	public static class AReducer extends Reducer<Text,Text,Text,Text>
	{
		public void reduce(Text key,Iterable<Text> values, Context ctxt)throws IOException,InterruptedException
		{
			StringBuilder sb=new StringBuilder();
			boolean isFirstItem=true;
			for(Text val:values)
			{   
				if(isFirstItem)
				{
					sb.append(val.toString());	
					isFirstItem=false;
				}
				else					
					break;
		     }
			ctxt.write(key, new Text(sb.toString()));			
		}
	}
	
	public static void main(String[] args) throws Exception
	{
		//CustomerProfileGen.runTest();
		
		int res = ToolRunner.run(new Configuration(),
				new AmazonProductMR(), args);
				
		System.exit(res);
		
	}
	
	//Produce sorted Product profile format is
	//ProductID||Title||Avg Rating|| SimilarProductIDs
	
	public int run(String[] args) throws Exception
	{		
		
		Configuration conf=new Configuration();		
    	//conf.set("mapreduce.output.textoutputformat.separator",",");	
    	conf.set("mapreduce.output.textoutputformat.separator","||");	
		Job job=Job.getInstance(conf,"AmazonCustomerMR");		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);		
		job.setMapperClass(AMapper.class);
		//job.setNumReduceTasks(0);
		job.setReducerClass(AReducer.class);		
		job.setInputFormatClass(ProductInputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));		
		FileOutputFormat.setOutputPath(job, new Path(args[1]));		
		return(job.waitForCompletion(true) ?0:1);
				
		
	}
	

}
