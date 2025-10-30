package it.polito.bigdata.hadoop.lab;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Lab - Reducer
 */

/* Set the proper data types for the (key,value) pairs */
class ReducerBigData2 extends Reducer<
                Text,           // Input key type
                Text,    // Input value type
                Text,           // Output key type
                DoubleWritable> {  // Output value type
    
    @Override
    protected void reduce(
        Text key, // Input key type
        Iterable<Text> values, // Input value type
        Context context) throws IOException, InterruptedException {

		/* Implement the reduce method */
        double sum = 0;
        int count = 0;

        for( Text value: values){
            double normalizeRating = Double.parseDouble(value.toString());
            sum += normalizeRating;
            count ++;
        }
    	if(count>0){
            double averageNormalizeRating = sum/count;
            context.write(key, new DoubleWritable(averageNormalizeRating));
        }
    }
}
