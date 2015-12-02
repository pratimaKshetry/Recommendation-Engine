/**
 *********************************************************************** 
 * Original Author Pratima Kshetry
 This code is filed user Creative Commons License.
 **********************************************************/

package PK;

import java.io.IOException;
import java.util.*;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class ClusterCombineMR extends Configured implements Tool {


	public static class ClusterMapper extends Mapper<Object, Text, Text, Text>
	{	
		private Text word1 = new Text();
		private Text word2 = new Text();	
		
	    public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
	    {		
						
			String strValue=value.toString();	
			//format of strValue 0807281956-0939173344:4-4,5-5
			//Extract key
			int ndx=strValue.indexOf(':');			
			//Remaining will be value
			String strKey=strValue.substring(0,ndx);
			String strVal=strValue.substring(ndx+1,strValue.length());
			//System.out.println(strKey+"->"+strVal);
		    word1.set(strKey);
		    word2.set(strVal);
			context.write( word1,word2);					 
       }   
	   
	}
	
	
	////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	

	public static class ClusterReducer extends Reducer<Text,Text,Text,FloatWritable>
	{
		
		
		//Corr(X,Y)=(n*sum(xy)-sum(x)sum(y)/[sqrt(n*sum(x^2)-(sum(x))^2)*sqrt(n*sum(y^2)-(sum(y))^2)]
		public static float correlate(float sum_x,float sum_y,float sum_xy,float sum_xx,float sum_yy, int n)
		{
			double corr=0;
			double numer=n*sum_xy- (sum_x*sum_y);
			
			float d11= n*sum_xx-(sum_x*sum_x);
			float d21= n*sum_yy-(sum_y*sum_y);
			//float d1=(float)Math.sqrt(d11);
			//float d2=(float)Math.sqrt(d21);
			double deno=Math.sqrt(d11*d21);
			
			if(deno!=0)
			{			
				corr=numer/deno;
			}
			return (float)corr;
		}
		
		private FloatWritable result = new FloatWritable();
		private Text word=new Text();
	    
	        
	    
	    public void reduce(Text key, Iterable<Text> values,  Context context) throws IOException, InterruptedException 
	    {
	    	    int n=0;
	    	    float sum_x=0, sum_y=0, sum_xy=0, sum_xx=0, sum_yy=0;
	    	    float corr=0;
	    	
	    	    
	    	   
	    		for (Text val : values) 
	    		{	
	    		    String strValue=val.toString();
	    		   //format of strValue 4-4,5-5
	    			String[] valuePair=strValue.split(",");
	    			//stVal should be of form 4-4
	    			for(String stVal:valuePair)
	    			{
		    			String[] stRatings=stVal.split("-");	    			
		    			if(stRatings.length==2)
		    			{
		    				try
		    				{
		    					float rating1=Float.parseFloat(stRatings[0]);
		    					float rating2=Float.parseFloat(stRatings[1]);
		    					sum_xx += rating1 * rating1;
		    					sum_yy += rating2 * rating2;
		    					sum_xy+= rating1 * rating2;
		    					sum_x+= rating1;
		    					sum_y+= rating2;
		    					n++; 				
		    				
		    				}
		    				catch(Exception e)
		    				{
		    					
		    				}    			
	  	    			}
	    			}
	    		}
	    		
	    		try
	    		{	    		
		    		
	    			if(n>4) //only if there have been more than four rating available for product pair proceed
	    			{
			    		corr=correlate(sum_x,sum_y,sum_xy,sum_xx,sum_yy,n);
						result.set(corr);
						String stKey=key.toString()+"|"+n;
						word.set(stKey);
					    context.write(word, result);	  
	    			}
	    		
	    		}
	    		catch(Exception e)
	    		{
	    		
	    		}

	    }		
		
	}
	
	
	
	////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	
	
	
	public static void main(String[] args) throws Exception
	{
			
		int res = ToolRunner.run(new Configuration(),
				new ClusterCombineMR(), args);				
		System.exit(res);		
	}
	
	public int run(String[] args) throws Exception
	{		
		
	    Configuration conf = new Configuration();
	    conf.set("mapreduce.output.textoutputformat.separator","|");
	    conf.setBoolean("mapreduce.output.compress", true);
	    conf.setBoolean("mapreduce.output.fileoutputformat.compress", true);
	    conf.setClass("mapreduce.map.output.compression.codec",GzipCodec.class,CompressionCodec.class);
	    Job job = Job.getInstance(conf, "clusterCombine");
	    job.setJarByClass(ClusterCombineMR.class);
	    //job.setNumReduceTasks(0);
	    job.setMapperClass(ClusterMapper.class);	    
	    job.setReducerClass(ClusterReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    FileOutputFormat.setCompressOutput(job, false);
	    FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
	    return(job.waitForCompletion(true) ?0:1);		
	}	
	
}

