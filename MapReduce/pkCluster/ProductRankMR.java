/**
 * 
 * Original Author Pratima Kshetry
 * 
 * The intention of this application is to cluster products on the basis of their correlation value
 * Given a product all the other products correlated with it is sorted according to correlation factor
 * Highly correlated products are ranked first and so forth
 * Thus the output of the reducer would be the basis of collaborative filtering for recommending the products.
 * Output of the reducer will be of following format
 * 0007154615:0007141076,1.0||0060098899,1.0||1402524994,0.9432422||1402511787,0.875||0786247924,0.62931675||1559277807,0.0||0060199652,0.0||0312422156,0.0||0694525596,-0.3227486||0375726403,-0.3227486
 * Above gives the list of correlated products for product 0007154615 as (x,y) pair where x is productID and y is correlation factor
 * Thus the output can be used as a model for recommending customers on the basis of product they have purchased. 
 * 
 * */

package pkcluster;

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


public class ProductRankMR extends Configured implements Tool {


	public static class ProductRankMapper extends Mapper<Object, Text, Text, Text>
	{	
		private Text word1 = new Text();
		private Text word2 = new Text();	
		
	    public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
	    {		
	    	String strValue=value.toString();	
			//format of strValue 	0001047655-0061007129|12|-0.07658396
			//Split data
	    	String[] strData=strValue.split("\\|");
	    	String outKey1="";
	    	String outKey2="";
	    	String outVal1="";
	    	String outVal2="";
	    	if(strData.length==3)
	    	{
	    		String[] keys=strData[0].split("-");
	    		if(keys.length==2)
	    		{
	    			outKey1=keys[0];
	    			outKey2=keys[1];
	    			outVal1=outKey2+"|"+strData[2];
	    			outVal2=outKey1+"|"+strData[2];
	    			
	    		    word1.set(outKey1);
	    		    word2.set(outVal1);
	    			context.write( word1,word2);
	    			
	    		    word1.set(outKey2);
	    		    word2.set(outVal2);
	    			context.write( word1,word2);
	    		}    		
	    		
	    	}					 
       }   
	   
	}
	
	
	////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	

	public static class ProductRankReducer extends Reducer<Text,Text,Text,Text>
	{
		
		private Text wordVal=new Text();
		private class DataPoint
		{
			private String data;
			private float  fCorr;
			public DataPoint(String data, String corr)
			{
				this.data=data;
				try
				{
					
					fCorr=Float.parseFloat(corr);
				
				}
				catch(Exception e)
				{
					fCorr=0;
				}			
				
			}
			
			public DataPoint(DataPoint other)
			{
				this.data=other.getData();
				this.fCorr=other.getCorrelation();
			}
			public String getData()
			{
				return data;
			}
			public float getCorrelation()
			{
				return fCorr;
			}
			public int compare(DataPoint other)
			{				
				if(this.getCorrelation()>other.getCorrelation()) return 1;
				if(this.getCorrelation()<other.getCorrelation()) return -1;
				return 0;				
			}
		}
		
		
        //in place insertsort of datapoints
		public Vector<DataPoint> SortByCoRrelation(Vector<DataPoint>dataPoints)
		{
			for(int i=1;i<dataPoints.size();i++)
			{
				int j=i;
				while(j>0 && dataPoints.get(j-1).compare(dataPoints.get(j))<0)
				{
					//DataPoint dp=new DataPoint(dataPoints.get(j));
					DataPoint dp=dataPoints.get(j);
					dataPoints.set(j, dataPoints.get(j-1));
					dataPoints.set(j-1, dp);					
					j=j-1;
				}
				
			}			
			return dataPoints;
		}
	        
	    
	    public void reduce(Text key, Iterable<Text> values,  Context context) throws IOException, InterruptedException 
	    {
		        
	    	    Vector<DataPoint> dataPoints=new Vector<DataPoint>();
	    	
	    	    for (Text val : values) 
	    		{	
	    		    String strValue=val.toString();
	    		    //format of strValue 	0061007129|-0.07658396
	    		    String[] strDataPoint=strValue.split("\\|");
	    		    if(strDataPoint.length==2)
	    		    {
	    		    	dataPoints.addElement(new DataPoint(strDataPoint[0],strDataPoint[1]));	    		    	
	    		    }
	    	
	    		}   
	    	    dataPoints=SortByCoRrelation(dataPoints);
	    	    String outValue="";
	    	    for(DataPoint dp:dataPoints)
	    	    {
	    	    	outValue+=dp.getData()+","+dp.getCorrelation()+"|";
	    	    }
	    	    outValue=outValue.substring(0,outValue.length()-1);
	    	    wordVal.set(outValue);
	    	    context.write(key, wordVal);

	    }		
		
	}
	
	
	
	////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	
	
	
	public static void main(String[] args) throws Exception
	{
			
		int res = ToolRunner.run(new Configuration(),
				new ProductRankMR(), args);				
		System.exit(res);		
	}
	
	public int run(String[] args) throws Exception
	{		
		
	    Configuration conf = new Configuration();
	    conf.set("mapreduce.output.textoutputformat.separator",":");
	   // conf.setBoolean("mapreduce.output.compress", true);
	    //conf.setBoolean("mapreduce.output.fileoutputformat.compress", true);
	   // conf.setClass("mapreduce.map.output.compression.codec",GzipCodec.class,CompressionCodec.class);
	    Job job = Job.getInstance(conf, "productRank");
	    job.setJarByClass(ProductRankMR.class);
	    //job.setNumReduceTasks(0);
	    job.setMapperClass(ProductRankMapper.class);	    
	    job.setReducerClass(ProductRankReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    //FileOutputFormat.setCompressOutput(job, false);
	   // FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
	    return(job.waitForCompletion(true) ?0:1);		
	}	
	
}


