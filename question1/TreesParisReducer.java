package cs.bigdata.Lab2.question1;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.IntWritable;

public class TreesParisReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private IntWritable nbTrees = new IntWritable();

    @Override
    public void reduce(final Text key, final Iterable<IntWritable> values,
            final Context context) throws IOException, InterruptedException {

        int nbTreesInt = 0;
        Iterator<IntWritable> iterator = values.iterator();
        while (iterator.hasNext()) {
        		nbTreesInt += iterator.next().get();
        }

        nbTrees.set(nbTreesInt);
        // Emit (type, #trees)
        context.write(key, nbTrees);
    }
}
