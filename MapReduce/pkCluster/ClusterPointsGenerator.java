/**
 * 
 * Original Author Pratima Kshetry
 * 
 * The intention of this application is to generate clusterPoints (R1,R2) rating pair for two products (P1,P2) for every two products 
 * rated by a given customer. 
 * Eg. If P1 and P2 are any two products rated by customer C as (R1,R2). The data Point is (R1,R2) for keys (P1,P2)
 * 
 * Since the data points to process and generate are too large. This application generates one output split for 6000 lines of customer 
 * profile being processed and grouped by (P1,P2) or if clusterPoints being generated for a single customer reaches more than 1000000 points
 * for eg. the output split will have following kind of info 
 *          ...
 * B000006O86-B00001U0DR:2-2,2-4
 * B000002OQH-B00005KARO:5-5
 *          ...
 * 
 * In above example product  B000006O86-B00001U0DR is rated by two customers within this split and their ratings were (2,2) and (2,4) respectively
 * In order to generate all these data points, any data point from the customer who has rated more than 10,000 products are ignored.
 * Such customer are flagged as scammers. Besides if such data points are considered, the data point combination will be huge with useless result
 * For n products rated there will be nC2 pair of data points nC2= n!/(n-2)!2!.For n=10000;nC2=49995000
 * 
 * 
 * 
 * 
 * */


package pkcluster;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Vector;
import java.io.BufferedReader;
import java.io.BufferedWriter;


public class ClusterPointsGenerator {
	
    private BufferedReader rdr=null;
    private BufferedWriter wrt=null;
    private BufferedWriter logWrt=null;
    private HashMap<String, Vector<String>> cluster=new HashMap<String,Vector<String>>();
    private long maxLineToProcess=6000;
    private long maxDataPoints=1000000;
    private long lineRead=0;
    private long  lSplit=0;
    private String customer="";    
    
	public void generateOutputSplits()
	{
		try
		{
			String outputFile="D:\\cluster\\output\\out"+lSplit+".txt";
			FileWriter file=new FileWriter(outputFile);	
			wrt=new BufferedWriter(file);
		    String outStr="";
		    for(String k:cluster.keySet())
		    {
		    	Vector<String> vals=cluster.get(k);
		    	outStr="";
		    	for(String s:vals)
		    	{
		    		outStr+=s+",";
		    	}
		    	outStr=outStr.substring(0,outStr.length()-1)+"\r\n";//remove last character
		    	wrt.write(k+":"+outStr);
		    }

		    wrt.close();
		    System.out.println("Commited split: "+lSplit);
		    lSplit++; 
			
		}
		catch(Exception ex)
		{
			ex.printStackTrace();
		}
		finally
		{
			cluster.clear();
			System.gc();//Force JVM garbage collection lot of memory needed to cluster data points
		}
		
	}
	 
	  
    public void StartClustering(String inFile,String outFile) throws IOException
	{  
		try
		{			
		   	
		    long lRecordLastIndex=0;		    
			rdr=new BufferedReader (new FileReader(inFile));
			logWrt=new BufferedWriter (new FileWriter("D:\\cluster\\log.txt"));	  	
		    String line="";
		    long lineReadTotal=0;
		    
		    while((line=rdr.readLine())!=null)
		    {
		    	cluster(line);
		    	lineReadTotal++;
		    	lineRead++;		    	
			    	
			  if(lineRead-lRecordLastIndex>10000)System.out.println("Record read:"+lineRead);	//just to observe how many points have been genereated	    	
			  
			  if(lineRead>maxLineToProcess)
		    	{
		    		System.out.println("Total lines of Record read: "+lineReadTotal++);
		    		generateOutputSplits();		    		
		    		lineRead=0; 	
		   		
		    	}
		    	
		    }		   
		    generateOutputSplits();
		    System.out.println("Finished...");	    
		    
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		finally
		{
			if (rdr!=null)rdr.close();
			if (wrt!=null)wrt.close();
			if (logWrt!=null)logWrt.close();
		}		

	}
    public void log(String msg)
    {
		try
		{
    	
			if(logWrt!=null)
			{
				logWrt.write("log:"+msg+"\r\n");
				logWrt.flush();
			}
		}
		catch(Exception e)
		{
			
		}
    	
    }
	
    public void cluster(String strValue) 
    {
		int ndx=strValue.indexOf(';');
		//remove customerID from the string first
		//Remaining will be productID;Rating-productID;Rating
		
		
		customer=strValue.substring(0,ndx);		
		String str1=strValue.substring(ndx+1,strValue.length());
		HashMap<String, String> oMap=new HashMap<String,String>();	
					
		String[] keyvalInfo=str1.split("-");
		int keyValCount=0;
		if(keyvalInfo.length>=2)//consider only if the customer has rated at least 2 products.
		{
			//Do not consider customer that have rated more than 10000 products this can be scammers
			if(keyvalInfo.length>10000)
			{
				//alert scammer			
				String msg=customer+","+keyvalInfo.length+",Scammer";
				System.out.println(msg);
				log(msg);				
			}
			else
			{
				if(keyvalInfo.length>100)
				{
					 //log customers with more than 100 products rated					
					String msg=customer+","+keyvalInfo.length+",NonScammer";
					log(msg);
				}
				for(String strKeyValInfo:keyvalInfo)
				{
					String[] strKeyVal=strKeyValInfo.split(";");
					if(strKeyVal.length==2)
					{   //Filter redundant review by same customer
						if(!oMap.containsKey((strKeyVal[0])))
					    {
							oMap.put(strKeyVal[0], strKeyVal[1]);
							keyValCount++;
						}											
					}
				}
				
				String[][]kvArray=new String[keyValCount][2];
				int currentKeyVal=0;
				for(String k1:oMap.keySet())
				{
					if(currentKeyVal<keyValCount)
					{
						kvArray[currentKeyVal][0]=k1;
						kvArray[currentKeyVal][1]=oMap.get(k1);
						currentKeyVal++;
					}				  
				}			
				String k="",v="";	
				int clusterPoint=0;
				for(int i=0;i<keyValCount;i++)
				{
					for(int j=i+1;j<keyValCount;j++)
					{
					     if(i==j) continue;
						 k=kvArray[i][0]+"-"+kvArray[j][0];
					     v=kvArray[i][1]+"-"+kvArray[j][1];
					     if(!cluster.containsKey(k))
					     {
					    	 Vector<String> val =new Vector<String>();
					    	 val.add(v);
					    	 cluster.put(k, val);	
					    	 clusterPoint++;
					    	 if(clusterPoint> maxDataPoints)
					    	 {
					    		 System.out.println("Max Data Points Reached");
					    		 generateOutputSplits();
					    		 clusterPoint=0;
					    	 }
					    	 
					     }
					     else
					     {
					    	 Vector<String> val =cluster.get(k);
					    	 val.add(v);
					     }					
					}
				}			
			}
			oMap.clear();
			oMap=null;
		}
    
    }



	public static void main(String[] args) {		
		try
		{
	       ClusterPointsGenerator app=new ClusterPointsGenerator();
	       String inputFile,outputFile;
	       inputFile="D:\\cluster\\input\\stage.txt";
	       outputFile="D:\\cluster\\output\\stage1.txt";
	       app.StartClustering(inputFile,outputFile);
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}

	}

}
