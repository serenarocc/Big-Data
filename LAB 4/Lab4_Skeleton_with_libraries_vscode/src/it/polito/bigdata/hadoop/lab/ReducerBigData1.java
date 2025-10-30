package it.polito.bigdata.hadoop.lab;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Lab - Reducer
 */

/* Set the proper data types for the (key,value) pairs */
class ReducerBigData1 extends Reducer<
                Text,           // Input key type
                Text,    // Input value type
                Text,           // Output key type
                Text> {  // Output value type
    
    @Override
    protected void reduce(
        Text key, // Input key type
        Iterable<Text> values, // Input value type
        Context context) throws IOException, InterruptedException {

		/* Implement the reduce method */
        List <String> votazioni = new ArrayList<>();
        double sum = 0;
        double count = 0;

        for(Text value :  values){
            String parts[] = value.toString().split(",");
            if( parts.length == 2){
                String productID = parts[0];
                int score = Integer.parseInt(parts[1]);
                votazioni.add(productID + "," + score);
                sum += score;
                count ++;
            }
        }


        if(count > 0){//utente ha valutato almeno un prodotto
            double userAvg = sum/count;

            for(String votazione : votazioni ){
                String parts [] = votazione.split(",");
                String productID = parts[0];
                int score = Integer.parseInt(parts[1]);
                double normalizzazioneVotazione = score - userAvg;

                context.write(new Text(productID), new Text(String.valueOf(normalizzazioneVotazione))); //---
                //esci da reducer 1 con (productID, normalizzazioneVotoPerQuelUtente)
            }

        }
    	
    }
}
