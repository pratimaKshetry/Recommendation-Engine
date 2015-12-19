package pkProductProfile;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;


public class ProductInputFormat  extends FileInputFormat<Text, Text>{
    private ProductProfileReader reader = null; 
        
   
    @Override
    public RecordReader<Text, Text> createRecordReader(
            InputSplit inputSplit, TaskAttemptContext attempt) throws IOException,
            InterruptedException {
        reader = new ProductProfileReader();
        reader.initialize(inputSplit, attempt);
        return reader;
    }
}