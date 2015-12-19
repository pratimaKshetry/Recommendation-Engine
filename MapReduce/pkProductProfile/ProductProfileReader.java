
	/* Original Author: Pratima Kshetry*******************************************************************************
	 * This RecordReader implementation is meant to parse Amazon data set aailable at
	 * The meta data is of following type
	 * 
	 * Sample data example********************************************************************************************
	 * 
	 * Id:   1
	   ASIN: 0827229534
	   title: Patterns of Preaching: A Sermon Sampler
	   group: Book
	   salesrank: 396585
	   similar: 5  0804215715  156101074X  0687023955  0687074231  082721619X
	   categories: 2
	    |Books[283155]|Subjects[1000]|Religion & Spirituality[22]|Christianity[12290]|Clergy[12360]|Preaching[12368]
	    |Books[283155]|Subjects[1000]|Religion & Spirituality[22]|Christianity[12290]|Clergy[12360]|Sermons[12370]
	   reviews: total: 2  downloaded: 2  avg rating: 5
	    2000-7-28  cutomer: A2JW67OY8U6HHK  rating: 5  votes:  10  helpful:   9
	    2003-12-14  cutomer: A2VE83MZF98ITY  rating: 5  votes:   6  helpful:   5

	****************************************************************************************************************/



package pkProductProfile;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;



import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class ProductProfileReader extends RecordReader<Text, Text> {
	private  BufferedReader reader=null;
	private  String inputLine=null;
	private  ProductProfile currentProduct=new ProductProfile();	
	private int count = 0;
	private boolean endofLine=false;

	public ProductProfileReader()
	{		
	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		if(reader!=null) reader.close();		
	}

	
	@Override
	public Text getCurrentKey() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		Text key=null;
		if(currentProduct!=null)
		{
			key=new Text(currentProduct.id); //key is customer id.
		}
		else
			key=new Text("Unknown"); //thiss should not happen implies data impurity
		return key;
	}
	

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		
		if(currentProduct!=null)
		{
			Text key=new Text(currentProduct.toString());
			return key;		
		}
		return new Text("");
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return count;
	}
    //Important method. It processes next key value to be fed to mapper by hadoop
	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		
		boolean isProcessComplete=processNextRecord();	
		if(!isProcessComplete){count++;return true;}
		else return false;	}


    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext attempt) throws IOException, InterruptedException {
        Path path = ((FileSplit) inputSplit).getPath();

        FileSystem fs = FileSystem.get(attempt.getConfiguration());
        FSDataInputStream fsStream = fs.open(path);
        reader = new BufferedReader(new InputStreamReader(fsStream), 1024*100);
        
        while ((inputLine = reader.readLine()) != null) {
            //Break at first indicator of ID
        	if(inputLine.startsWith("Id:")){
                break;
            }
        }
    }
    //returns true if all the lines processed
	private boolean processNextRecord()throws IOException
	{
		if(endofLine)return true;
		String line=reader.readLine();		
		if(line==null) return true;		
		while(line!=null && !line.startsWith("Id:"))
		{			
			parseLine(line); //Important parses each line
			line=reader.readLine();
			if(line==null) endofLine=true;
		}
		return false;		
	}
	
	
	private void parseLine(String input)
	{
		//Implement regular expression to parse and product profile
		input=input.trim();
		if(input.startsWith("ASIN:"))
		{
			currentProduct.id=extractProductID(input);
		}
		if(input.startsWith("title:")) 
		{
			currentProduct.title="\""+extractProductTitle(input)+"\"";;
			
		}
		if(input.startsWith("similar:"))
		{
			currentProduct.similarProductIDs=extractSimilarProduct(input);			
		}		

		if(input.contains("avg rating:"))
		{
			currentProduct.avgRating=extractProductAvgRating(input);						
		}
	}
	
	private String extractProductAvgRating(String input)
	{
		String extractedAvgRating=null;
		String token="avg rating:";
		if(input==null)
		{
			return null;
		}
		input=input.trim();
		
		if(input.contains(token))
		{
			int pos=input.indexOf(token);
			extractedAvgRating=input.substring(pos+token.length());
			if(extractedAvgRating!=null)
			{				
				extractedAvgRating=extractedAvgRating.trim();
			}
		}
		return extractedAvgRating;
	}
	
	
	
	private String extractSimilarProduct(String input)
	{
		//format
		//similar: 5  0804215715  156101074X  0687023955  0687074231  082721619X
		//first number is count of similar items
		String extractedProductList=null;
		if(input==null) return null;
		input=input.trim();
		if(input.startsWith("similar:"))
		{
			int pos=input.indexOf(':');
			extractedProductList=input.substring(pos+1);
			if(extractedProductList!=null)
			{
				extractedProductList=extractedProductList.trim();
				String[] productIDlist=extractedProductList.split("\\s+");
				extractedProductList="";
				for(int i=1;i<productIDlist.length;i++)
				{
					extractedProductList+=productIDlist[i]+" "; //space separated
				}
			}			
		}
		return extractedProductList;
	}
	
	private String extractProductID(String input)
	{
		String extractedText=null;
		if(input!=null && input.startsWith("ASIN:"))
		{
			int pos=input.indexOf(':');
			extractedText=input.substring(pos+1);
			if(extractedText!=null)
			{
				extractedText=extractedText.trim();
			}
		}
		return extractedText;
	}
	
	private String extractProductTitle(String input)
	{
		String extractedText=null;
		if(input==null) return null;
		input=input.trim();
		if(input.startsWith("title:"))
		{
			int pos=input.indexOf(':');
			extractedText=input.substring(pos+1);
			if(extractedText!=null)
			{
				extractedText=extractedText.trim();
			}
		}
		return extractedText;
	}

}
