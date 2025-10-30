package it.polito.bigdata.hadoop.lab;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import it.polito.bigdata.hadoop.lab.WordCountWritable;
import it.polito.bigdata.hadoop.lab.TopKVector;

/**
 * Lab  - Mapper
 */

/* Set the proper data types for the (key,value) pairs */
class MapperBigData2 extends Mapper<
                    Text,                 // Input key type
                    Text,                 // Input value type
                    NullWritable,         // Output key type
                    WordCountWritable> {               // Output value type
    
    private TopKVector<WordCountWritable> localTop100;

    protected void setup(Context context) throws IOException, InterruptedException 
    {
        localTop100 = new TopKVector<WordCountWritable>(100);
    }
    
    protected void map(
            Text key,           // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException
    {
        localTop100.updateWithNewElement(new WordCountWritable(new String(key.toString()), new Integer(Integer.parseInt(value.toString()))));
    }

    protected void cleanup(Context context) throws IOException, InterruptedException 
    {
        for(WordCountWritable p : localTop100.getLocalTopK())
            context.write(NullWritable.get(), new WordCountWritable(p));
    }
}