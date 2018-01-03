package cs.bigdata.Lab2.question2;

import org.apache.hadoop.mapreduce.Job;  
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.Iterator;


public class TreesParisReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {

    private FloatWritable maxHeight = new FloatWritable();

    @Override
    public void reduce(final Text key, final Iterable<FloatWritable> values,
            final Context context) throws IOException, InterruptedException {

        float maxHeightFloat = 0;
        Iterator<FloatWritable> iterator = values.iterator();

        while (iterator.hasNext()) {
        		float candidate = iterator.next().get();
        		if (candidate>maxHeightFloat) {
        			maxHeightFloat = candidate;
        		}
        }

        maxHeight.set(maxHeightFloat);
        // Emit (type, maxHeight)
        context.write(key, maxHeight);
    }
}
