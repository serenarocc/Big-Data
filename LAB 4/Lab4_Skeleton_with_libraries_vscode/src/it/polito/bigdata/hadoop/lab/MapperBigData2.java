package it.polito.bigdata.hadoop.lab;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Lab  - Mapper
 */

/* Set the proper data types for the (key,value) pairs */
class MapperBigData2 extends Mapper<
                    Text, // Input key type
                    Text,         // Input value type
                    Text,         // Output key type
                    Text> {// Output value type
    
    protected void map(
            Text key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

    		/* Implement the map method */ 
            //al mapper 2 arrivano in ingresso (productID, normalizzazioneVotoPerQuelUtente)
            //String line  = value.toString();
            //String fields[] = line.split(",");
            
            //String productId = fields[0];
            //String normalizeRating = fields[1];

            //String productId = key.get();
            //String normalizeRating = value.get();

            //context.write(new Text(productId), new Text(normalizeRating));
            context.write(key, value);

    }
}
