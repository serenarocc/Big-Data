package it.polito.bigdata.hadoop.lab;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Lab - Reducer
 */

/* Set the proper data types for the (key,value) pairs */
class ReducerBigData1 extends Reducer<
                Text,           // Input key type
                Text,           // Input value type
                Text,           // Output key type
                Text> {         // Output value type
    
    @Override
    protected void reduce(
        Text key,                               // Input key type
        Iterable<Text> values,                  // Input value type
        Context context) throws IOException, InterruptedException {
        
        int sum = 0;

        for(Text value : values)
        {
            sum += Integer.parseInt(value.toString());
        }

        context.write(key, new Text(Integer.toString(sum)));
    }
}
