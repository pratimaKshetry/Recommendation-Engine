/* Original Author: Pratima Kshetry*********************************
*****Generates customer profile using MapReduce****************************/

package pkCustomerProfile;

import java.io.IOException;
import java.util.*;


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


public class AmazonCustomerMR extends Configured implements Tool {

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
			String valString="";
			int count=0;
			
			for(Text val1:values)
			{
				count++;
				valString+=val1.toString()+"-";				
				
			}
			if(count>4)
			ctxt.write(key,new Text(valString));
		}
		
		public void reduceClustering(Text key,Iterable<Text> values, Context ctxt)throws IOException,InterruptedException
		{
			//Format: ProductID;Rating;Title;SimilarProductList (Space Separated)
			String[] val1Info;	
			String[] val2Info;
			String valString="";
			String keyString="";
			Vector<String> valueList=new Vector<String>();			
			for(Text val1:values)
			{
				valueList.add(val1.toString());
			}
			for(int i=0;i<valueList.size();i++)
			{				
				val1Info=valueList.get(i).toString().split(";");
				for(int j=0;j<valueList.size();j++)
				{   
					try
					{
						if(i!=j)
						{   
							keyString="";
							valString="";
							val2Info=valueList.get(j).toString().split(";");							
							//Proceed only those have valid splits, else it can be invalid data						
							if(val1Info.length==2 && val2Info.length==2)
							{   
								keyString+=val1Info[0]+","+val2Info[0];
								//valString+=val1Info[1]+","+val2Info[1]+","+val1Info[2]+","+val2Info[2]+","+val1Info[3];
								valString+=val1Info[1]+","+val2Info[1];

								ctxt.write(new Text(keyString), new Text(valString));
							}
						}
					}
					catch(Exception ex)
					{
						ex.printStackTrace();
					}
				}				
			}	
		}	    	
	}
	
	
	public static void main(String[] args) throws Exception
	{
		int res = ToolRunner.run(new Configuration(),
				new AmazonCustomerMR(), args);				
		System.exit(res);		
	}
	
	public int run(String[] args) throws Exception
	{		
		
		Configuration conf=new Configuration();	
		conf.set("mapreduce.output.textoutputformat.separator",";");		
		Job job=Job.getInstance(conf,"AmazonCustomerMR");		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);		
		job.setMapperClass(AMapper.class);
		job.setReducerClass(AReducer.class);		
		job.setInputFormatClass(CustomerInputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));		
		FileOutputFormat.setOutputPath(job, new Path(args[1]));	
		
		/*
		 * Running Hadoop 2.6 from cmd line following should be the way to obtain right args.If running from eclipse use the above method
		 * 
		 * FileInputFormat.addInputPath(job, new Path(args[1]));		
		 * FileOutputFormat.setOutputPath(job, new Path(args[2]));
		*/
		
		return(job.waitForCompletion(true) ?0:1);			
		
	}

}
