package it.polito.bigdata.hadoop.lab;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import it.polito.bigdata.hadoop.lab.WordCountWritable;
import it.polito.bigdata.hadoop.lab.TopKVector;

/**
 * Lab - Reducer
 */

/* Set the proper data types for the (key,value) pairs */
class ReducerBigData2 extends Reducer<
                NullWritable,           // Input key type
                WordCountWritable,        // Input value type
                Text,                   // Output key type
                IntWritable> {          // Output value type

    @Override
    protected void reduce(
        NullWritable key,                           // Input key type
        Iterable<WordCountWritable> values,                      // Input value type
        Context context) throws IOException, InterruptedException
    {
        TopKVector<WordCountWritable> top100 = new TopKVector<WordCountWritable>(100);

        for(WordCountWritable pair : values)
        {
            top100.updateWithNewElement(new WordCountWritable(pair));
        }

        for(WordCountWritable pair : top100.getLocalTopK())
        {
            context.write(new Text(pair.getWord()), new IntWritable(pair.getCount()));
        }
    }
}
