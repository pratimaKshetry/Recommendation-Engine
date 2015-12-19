/******************
Original Author: Pratima Kshetry
*/

package pkEvaluation;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.*;

public class Recommender {
	
	private String customerProfilePath="";
	private String productProfilePath="";
	private String rankedProductPath="";
	
	private class Product {
		public String id;
		public String title;
		public String rating;	
		public String similarProductIDs;
		public String toString()
		{
			String st="ID: "+id+"\nTitle: "+title+"\nAvgRating: "+rating+"\nSimilar Products: "+ similarProductIDs;
			return st;
		}
	}
	
	public class RatedProducts {
		public String id;	
		public String rating;	
		public RatedProducts(String id, String rating)
		{
			this.id=id;
			this.rating=rating;
		}
		public String toString()
		{
			String st="("+id+","+rating+")";
			return st;
		}
	}
	
	public class CorrelatedProduct {
		public String id;	
		public String correlation;	
		public CorrelatedProduct(String id, String correlation)
		{
			this.id=id;
			this.correlation=correlation;
		}
		public String toString()
		{
			//String st="("+id+","+correlation+")";
			String st=""+id+","+correlation;
			return st;
		}
	}
	
	public class Customer {
		public String id;
		//public Vector<RatedProducts> productList=new Vector<RatedProducts>();	
		public HashMap<String,RatedProducts> productListMap= new HashMap<String,RatedProducts>();	
		/*public String toString()
		{
			StringBuilder sb=new StringBuilder();
			sb.append("Customer:"+id+"\n");
			int count=0;			
			for(int i=0;i<productList.size();i++)
			{
				
				RatedProducts rp=productList.get(i);
				sb.append(rp.toString());
				sb.append(",");
				if(count>5)
				{
					sb.append("\n");
					count=0;
				}
				count++;
			}
			return (sb.toString().substring(0,sb.length()-1));
		}*/
		
		
		public String toString()
		{
			StringBuilder sb=new StringBuilder();
			sb.append("Customer:"+id+"\n");
			int count=0;
			Vector<RatedProducts> pdList=GetProductList();
			for(int i=0;i<pdList.size();i++)
			{
				
				RatedProducts rp=pdList.get(i);
				sb.append(rp.toString());
				sb.append(",");
				if(count>5)
				{
					sb.append("\n");
					count=0;
				}
				count++;
			}
			return (sb.toString().substring(0,sb.length()-1));
		}
		
		public Vector<RatedProducts> GetProductList()
		{
			Vector<RatedProducts> ratedList=new Vector<RatedProducts>(productListMap.values());
			return ratedList;
		}
	}

	private HashMap<String,Customer> customerTbl=null; 
	private HashMap<String,Vector<CorrelatedProduct>> correlatedTbl=null; 
	private HashMap<String,Product> productTbl=null; 
	
	
	public Recommender(String customerProfilePath,String productProfilePath, String rankedProductPath)
	{
		this.customerProfilePath=customerProfilePath;
		this.productProfilePath=productProfilePath;
		this.rankedProductPath=rankedProductPath;	
		customerTbl= new HashMap<String,Customer>();
		correlatedTbl=new HashMap<String,Vector<CorrelatedProduct>>();
		productTbl=new HashMap<String,Product>();
	}
	
	
	private void ParseAndLoadCustomer(String line)
	{
		int nPos=line.indexOf(";");
		String customerID=line.substring(0,nPos);
		line=line.substring(nPos+1,line.length());
		String[] ratedProducts=null;
		String[] product=null;
		if(!customerTbl.containsKey(customerID))
		{
			Customer cust=new Customer();
			cust.id=customerID;
			ratedProducts=line.split("-");
			for(int i=0;i<ratedProducts.length;i++)
			{
				product=ratedProducts[i].split(";");
				if(product.length==2)
				{   
					try
					{
						RatedProducts pd=new RatedProducts(product[0],product[1]);
						//avoid duplicate product
						if(!cust.productListMap.containsKey(product[0]))
						{
							cust.productListMap.put(product[0], pd);
						}
				
					}
					catch (Exception e)
					{
						e.printStackTrace();
					}
				}
				
			}
			customerTbl.put(customerID,cust);
			
		}
	}

	public void ReadCustomerTbl()
	{
		try
		{
			BufferedReader rdr=new BufferedReader (new FileReader(this.customerProfilePath));
			int lineReadTotal=0;
			if(rdr!=null)
			{
			    String line="";
				while((line=rdr.readLine())!=null)
			    {
					ParseAndLoadCustomer(line);				
					
			    	lineReadTotal++;
			    }	    	

			       
			}
			rdr.close();
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		
	}
	
	public void DisplayCustomerTbl()
	{		
		Iterator<String> it= customerTbl.keySet().iterator();
		while(it.hasNext())
		{
			Customer ct=customerTbl.get(it.next());
			System.out.println(ct.toString());
		}
	}
	
	
	public void DisplayCustomerTbl(String customerID)
	{
		
		Customer ct=customerTbl.get(customerID);	
		if(ct!=null)
		System.out.println(ct.toString());		
	}
	
	public void Recommend(String customerID)
	{
		
		Customer ct=customerTbl.get(customerID);	
		if(ct!=null)
		{
			Vector<RatedProducts> productList=ct.GetProductList();
			for(RatedProducts pd:productList)
			{
				DisplayRecommendationTbl(pd.id);
			}
		
		}
	}
	
	public void Recommend(String customerID, String corr)
	{
		
		Customer ct=customerTbl.get(customerID);	
		if(ct!=null)
		{
			Vector<RatedProducts> productList=ct.GetProductList();
			for(RatedProducts pd:productList)
			{
				DisplayRecommendationTbl(pd.id, corr);
			}
		
		}
	}
	
	public void RecommendExport(String customerID, String corr, String exportPath)
	{
		try
		{
			Customer ct=customerTbl.get(customerID);	
			if(ct!=null)
			{
				Vector<RatedProducts> productList=ct.GetProductList();
				for(RatedProducts pd:productList)
				{
					DisplayRecommendationTbl(pd.id, corr,exportPath);
				}
			
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	
	public void RecommendExportVerbose(String customerID, String corr, String exportPath)
	{
		try
		{
			Customer ct=customerTbl.get(customerID);	
			if(ct!=null)
			{
				Vector<RatedProducts> productList=ct.GetProductList();
				for(RatedProducts pd:productList)
				{
					DisplayRecommendationTblVerbose(pd.id, corr,exportPath);
				}
			
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	
	public void DisplayRandomCustomer()
	{
		int custSize=customerTbl.size();
		Random rand=new Random();
		int randomNumber=rand.nextInt(custSize);
		List<String> keys=new ArrayList<String>(customerTbl.keySet());
		String customerID=keys.get(randomNumber);		
		DisplayCustomerTbl(customerID);
		
	}
	
	public void DisplayRandomCustomer(String strMinProductRated)
	{
		int minProductRated=0;
		try
		{
			minProductRated=Integer.parseInt( strMinProductRated);
		}
		catch(Exception e1)
		{
			e1.printStackTrace();
			minProductRated=0;
		}
		int custSize=customerTbl.size();
		boolean bStop=false;
		String customerID="";
		while(!bStop)
		{
			Random rand=new Random();
			int randomNumber=rand.nextInt(custSize);
			List<String> keys=new ArrayList<String>(customerTbl.keySet());
			customerID=keys.get(randomNumber);	
			
			Customer cust=customerTbl.get(customerID);
			if(cust.productListMap.size()>=minProductRated) bStop=true;
		}
		DisplayCustomerTbl(customerID);
		
	}
	
	private void ParseAndLoadRankedProducts(String line)
	{
		int nPos=line.indexOf(":");
		Vector<CorrelatedProduct> cps=new Vector<CorrelatedProduct>();
		String productID=line.substring(0,nPos);
		line=line.substring(nPos+1,line.length());
		String[] correlatedProducts=null;
		String[] product=null;
		if(!correlatedTbl.containsKey(productID))
		{
			correlatedProducts=line.split("\\|");
			for(int i=0;i<correlatedProducts.length;i++)
			{
				product=correlatedProducts[i].split(",");
				if(product.length==2)
				{   
					try
					{
						CorrelatedProduct pd=new CorrelatedProduct(product[0],product[1]);
						cps.add(pd);
					}
					catch (Exception e)
					{
						e.printStackTrace();
					}
				}
				
			}
			correlatedTbl.put(productID,cps);			
		}
	}
	
	public void ReadRecommendationTbl()
	{
		try
		{
			BufferedReader rdr=new BufferedReader (new FileReader(this.rankedProductPath));
			int lineReadTotal=0;
			if(rdr!=null)
			{
			    String line="";
				while((line=rdr.readLine())!=null)
			    {
					ParseAndLoadRankedProducts(line);				
			    	lineReadTotal++;
			    }  	

			}
			rdr.close();
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		
	}
	public void DisplayRecommendationTbl()
	{
		
		Iterator<String> it= correlatedTbl.keySet().iterator();
		while(it.hasNext())
		{
			String prodID=it.next();
			Vector<CorrelatedProduct> cps=correlatedTbl.get(prodID);
			System.out.println("*******Product: "+prodID+"*******");
			int count=0;
			for(CorrelatedProduct cp:cps)
			{   count++;
				System.out.print(cp.toString());
				if(count>5)
				{
					System.out.println("");
					count=0;
				}
			}
		}
		
	}
	
	public void DisplayRecommendationTbl(String prodID)
	{
		
		Vector<CorrelatedProduct> cps=correlatedTbl.get(prodID);	
		if(cps!=null)
		{
			System.out.println("\nRecommended Product for "+prodID);			
			for(CorrelatedProduct cp:cps)
			{   
				System.out.println(cp.toString());
			}	
		}
	}
	
	public void DisplayRecommendationTbl(String prodID,String minCorr)
	{
		
		Vector<CorrelatedProduct> cps=correlatedTbl.get(prodID);	
		if(cps!=null)
		{
			System.out.println("\nRecommended Product for "+prodID);			
			for(CorrelatedProduct cp:cps)
			{   
				
				try
				{
				    float fMinCorr=Float.parseFloat(minCorr);
				    float fCorr=Float.parseFloat(cp.correlation);
				    if(fCorr>=fMinCorr)
					System.out.println(cp.toString());
				}
				catch(Exception e)
				{
					e.printStackTrace();
				}
			}	
		}
	}
	

	
	public void DisplayRecommendationTbl(String prodID,String minCorr,String exportPath) throws Exception
	{
		FileWriter outFile=null;
		
		try
		{
			outFile=new FileWriter(exportPath,true);
			HashMap<String,String> recommendedProduct=  new HashMap<String,String>();
			
			Vector<CorrelatedProduct> cps=correlatedTbl.get(prodID);
			String stWrite=null;
			if(cps!=null)
			{
				stWrite="\nRecommended Product for "+prodID+"\r\n";
				System.out.println(stWrite);
				outFile.write(stWrite);
				for(CorrelatedProduct cp:cps)
				{   
					if(!recommendedProduct.containsKey(cp.id))//do not print item already recommended
					{
						try
						{
						    float fMinCorr=Float.parseFloat(minCorr);
						    float fCorr=Float.parseFloat(cp.correlation);
						    if(fCorr>=fMinCorr)
						    {
						    	recommendedProduct.put(cp.id,"");
						    	stWrite=cp.toString();
						    	System.out.println(stWrite);
						    	outFile.write(stWrite+"\r\n");
						    	outFile.flush();					    	
						    }
						}
						catch(Exception e)
						{
							e.printStackTrace();
						}
					}
				}	
			}
			recommendedProduct.clear();
			recommendedProduct=null;
		}
		catch(Exception ex)
		{
			ex.printStackTrace();
		}
		finally
		{
			if(outFile!=null) outFile.close();
		}
	}
	
	
	public void DisplayRecommendationTblVerbose(String prodID,String minCorr,String exportPath) throws Exception
	{
		FileWriter outFile=null;
		
		try
		{
			outFile=new FileWriter(exportPath,true);
			HashMap<String,String> recommendedProduct=  new HashMap<String,String>();
			
			Vector<CorrelatedProduct> cps=correlatedTbl.get(prodID);
			String stWrite=null;
			if(cps!=null)
			{
				stWrite="\r\n\r\nRecommendeded Product for ID:"+prodID+" Title: ";
				
				if (productTbl.containsKey(prodID))
				{
					Product p=productTbl.get(prodID);
					stWrite+=p.title;								
				}				
				stWrite+="\r\n";
				stWrite+="*************************************************************************************************************************************\r\n";
				
				System.out.println(stWrite);
				outFile.write(stWrite);				
	
				for(CorrelatedProduct cp:cps)
				{   
					if(!recommendedProduct.containsKey(cp.id))//do not print item already recommended
					{
						try
						{
						    float fMinCorr=Float.parseFloat(minCorr);
						    float fCorr=Float.parseFloat(cp.correlation);
						    if(fCorr>=fMinCorr)
						    {
						    	recommendedProduct.put(cp.id,"");
						    	stWrite=cp.toString();
								if (productTbl.containsKey(cp.id))
								{
									Product p=productTbl.get(cp.id);
									stWrite+=","+p.title;
								}
						    	
						    	System.out.println(stWrite);
						    	outFile.write(stWrite+"\r\n");
						    	outFile.flush();					    	
						    }
						}
						catch(Exception e)
						{
							e.printStackTrace();
						}
					}
				}				
				System.out.println(stWrite);
				outFile.write(stWrite);
			}
			recommendedProduct.clear();
			recommendedProduct=null;
		}
		catch(Exception ex)
		{
			ex.printStackTrace();
		}
		finally
		{
			if(outFile!=null) outFile.close();
		}
	}
	
	/*
	 * 0000037931,"Saluki Champions, 1952-1988",0,
       0001047655,"Prodigal Daughter",4,0061007129 0061007358 0061007137 
	 */
	
	private void ParseAndLoadProductProfile(String line)
	{		
		Product prod=new Product();
		String[] lines=line.split("\\|\\|");
		if(lines.length==4)
		{
			prod.id=lines[0];
			prod.title=lines[1];
			prod.rating=lines[2];
			prod.similarProductIDs=lines[3];
			if(!productTbl.containsKey(prod.id))
			{
				productTbl.put(prod.id,prod);
			}
		}
		else
		{   //just extract id and title if more info is not present
			if(lines.length>=2)
			{
				prod.id=lines[0];
				prod.title=lines[1];
				prod.rating="";
				prod.similarProductIDs="";
				if(!productTbl.containsKey(prod.id))
				{
					productTbl.put(prod.id,prod);
				}
			}
			
		}
		
		
		
	}
	
	public void ReadProductTbl()
	{
		try
		{
			BufferedReader rdr=new BufferedReader (new FileReader(this.productProfilePath));
			int lineReadTotal=0;
			if(rdr!=null)
			{
			    String line="";
				while((line=rdr.readLine())!=null)
			    {
					ParseAndLoadProductProfile(line);					
			    	lineReadTotal++;
			    }
				    	

			    //System.out.println("Finished...");	    
			}
			rdr.close();
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		
	}
	
	public void DisplayProductProfile(String prodID)
	{
		if (productTbl.containsKey(prodID))
		{
			Product p=productTbl.get(prodID);
			System.out.println(p.toString());
		}
	}
	
	
	
	
	public static boolean parseAndRun(Recommender reco, String cmd)
	{
		boolean bRet=false;
		String[] commands=cmd.split(" ");
		
		switch(commands[0])
		{
		   case "customer":
			   if(commands.length>1)
			   {
				   reco.DisplayCustomerTbl(commands[1]);
			   }
			
			   break;
			   
		   case "rank":
			   if(commands.length>1)
			   {
				   reco.DisplayRecommendationTbl(commands[1]);
			   }
			
			   break;
			   
		   case "recommend":			   
			   if(commands.length==2)
			   {
				   reco.Recommend(commands[1]);
			   }
			  if(commands.length==3)
			  {
				  reco.Recommend(commands[1],commands[2]);
			  }
			  
		   case "recommendexport":			   
	
			  if(commands.length==4)
			  {
				  reco.RecommendExport(commands[1],commands[2],commands[3]);
			  }
			   
	
		   break;
		   case "recommendexportV":
			   
		      if(commands.length==4)
			  {
			     reco.RecommendExportVerbose(commands[1],commands[2],commands[3]);
			  }
			   
		   break;
		   
		   
		   case "customer?":
			   if(commands.length==1)
			   {
				   reco.DisplayRandomCustomer();
			   }
			   
			   if(commands.length==2)
			   {
				   reco.DisplayRandomCustomer(commands[1]);
			   }
			   break;
			   
			   
		   case "customer*":			   
				   reco.DisplayCustomerTbl();	  
		
			   break;   
			   
		   case "rank*":			   
			   reco.DisplayRecommendationTbl();  
	
		   break; 
		   
		   case "product":
			   if(commands.length==2)
			   {
				   reco.DisplayProductProfile(commands[1]);
			   }
		   break;
		   case "exit":
			   System.out.println("Goodbye..");
			   bRet=true;
			   break;
			   
		   case "help":
			   System.out.println("Commands:");
			   System.out.println("customer <customerID> : Display products rated by given customer");
			   System.out.println("product <productID> : Display product profile of given product");
			   System.out.println("rank <productID> : Display correlated product of given product");
			   System.out.println("customer* : Display products rated by all customers");
			   System.out.println("customer? : Display products rated by any one random customer");
			   System.out.println("customer? <minProductRated> : Display products rated by any one random customer with at least minimum product rated=minProductRated");
			   System.out.println("rank* : Display all correlated products");
			   System.out.println("recommend <customerID> : Display universe of all correlated products of products bought by customer");
			   System.out.println("recommend <customerID> [correlation]: Display universe of all correlated products of products bought by customer with given minimum correlation factor");
			   System.out.println("recommendexport <customerID> <correlation> <exportFilePath>: Display universe of all correlated products of products bought by customer with given minimum correlation factor and export them to given file");
			   System.out.println("recommendexportV <customerID> <correlation> <exportFilePath>: Display universe of all correlated products of products bought by customer with given minimum correlation factor also include title of product and export them to given file");
		   default:
				System.out.println("Unknown command");
		
		}
		
	
		return bRet;
	}
	
	public static void main(String[] args) {		
		try
		{
		  // Recommender app=new Recommender("D:\\AmazonRecoPK\\data\\Evaluation\\customer.txt","D:\\AmazonRecoPK\\data\\Evaluation\\product.txt","D:\\AmazonRecoPK\\data\\Evaluation\\ranked.txt");
		   Scanner scIn= new Scanner(System.in);
		   String input=null;
		   String customerProfilePath="D:\\AmazonRecoPK\\data\\Evaluation\\customer.txt";
		   String productProfilePath="D:\\AmazonRecoPK\\data\\Evaluation\\product.txt";
		   String rankedPath="D:\\AmazonRecoPK\\data\\Evaluation\\ranked.txt";
		   
		   System.out.println("\nDefault Profiles Path are:");
		   System.out.println(customerProfilePath);
		   System.out.println(productProfilePath);
		   System.out.println(rankedPath);
		   System.out.println("\nLoad Default Profiles Y/N>");
		   input=scIn.nextLine();
		   if(input.compareToIgnoreCase("N")==0)
		   {		   
			   System.out.print("\nEnter CustomerProfilePath>");
			   input=scIn.nextLine();
			   customerProfilePath=input.trim();
			   
			   System.out.print("\nEnter ProductProfilePath>");
			   input=scIn.nextLine();
			   productProfilePath=input.trim();
			   
			   System.out.print("\nEnter RankedProductsPth>");
			   input=scIn.nextLine();
			   rankedPath=input.trim();
		   }
		  // scIn.close();
		   
		   Recommender app=new Recommender(customerProfilePath,productProfilePath,rankedPath);
		   
		   
		   System.out.println("Loading customer profile....");
		   app.ReadCustomerTbl();	
		   System.out.println("Loading recommendation table....");
		   app.ReadRecommendationTbl();	
		   System.out.println("Loading product table....");
		   app.ReadProductTbl();
		   System.out.println("Data Read Complete....");
		   //app.DisplayProductProfile("0553288733");

		   boolean bExit=false;
		   //Scanner sc= new Scanner(System.in);
		   
		   while(!bExit)
		   {
			   System.out.print("\nReco>");
			   input=scIn.nextLine();
			   bExit=Recommender.parseAndRun(app,input);			   
		   }
		   scIn.close();
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}

	}

}
