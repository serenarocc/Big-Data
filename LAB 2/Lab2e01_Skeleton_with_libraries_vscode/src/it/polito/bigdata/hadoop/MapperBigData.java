package it.polito.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Lab  - Mapper
 * si usa Text perché il programma sta leggendo e riscrivendo coppie word\tcount come testo puro 
 * (word come chiave testuale e count come stringa). In particolare si è scelto
 *  KeyValueTextInputFormat — che prende tutto ciò che sta prima del separatore (default \t) come chiave
 *  Text e tutto ciò che sta dopo come valore Text — quindi ha molto senso dichiarare sia input key che input
 *  value come Text. Allo stesso modo l'output viene scritto come testo (file con righe key\tvalue), quindi si 
 * usa TextOutputFormat e tipi Text
 */

/* Set the proper data types for the (key,value) pairs */
class MapperBigData extends Mapper<
                    Text, // Input key type. --> riga lunga -> key è l'offset ->type string
                    Text,         // Input value type --> tab usato come separator -> value è il contenuto della risga
                    Text,         // Output key type
                    Text> {// Output value type
    
    String prefix;

	protected void setup(Context context) {
		prefix = context.getConfiguration().get("prefix").toString();
	}

    protected void map(
            Text key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

    		/* Implement the map method */ 

           if(key.toString().toLowerCase().startsWith(prefix)){
                context.write(key, value);
                
           }
    }
}
