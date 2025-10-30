package it.polito.bigdata.hadoop.lab;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import it.polito.bigdata.hadoop.lab.WordCountWritable;

/**
 * Lab  - Mapper
 */

/* Set the proper data types for the (key,value) pairs */
class MapperBigData1 extends Mapper<
                    LongWritable, // Input key type
                    Text,         // Input value type
                    Text,         // Output key type
                    Text> {       // Output value type
    
    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

    		String[] fields = value.toString().split(",");

            for(int i = 1; i < fields.length; i++)
            {
                for(int j = 1; j < fields.length; j++)
                {
                    if(fields[i].compareTo(fields[j]) < 0)
                    {
                        WordCountWritable obj = new WordCountWritable(fields[i].concat(",").concat(fields[j]), 1);
                        context.write(new Text(obj.getWord()), new Text(obj.getCount().toString()));
                    }
                }
            }
    }
}
