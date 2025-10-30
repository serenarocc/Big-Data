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
class MapperBigData1 extends Mapper<
                    LongWritable, // Input key type
                    Text,         // Input value type
                    Text,         // Output key type
                    Text> {// Output value type
    
    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

    		/* Implement the map method */
            //devo togliere la prima riga con i nomi dei campi, non mi serve
            if(key.get()==0){
                return;
            } 

            String line = value.toString();
            // String fields[] = line.split(",");
            String[] fields = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);

            
            if (fields.length >= 7) {

            String userID = fields[2];
            String productID = fields[1];
            String score = fields[6];
            
            context.write(new Text(userID), new Text(productID + "," + score));
            //ogni riga diventa -> (userID, (productID,score))
             }
    }
}
