package cs.bigdata.Lab2.question1;
import org.apache.hadoop.io.*;        
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class TreesParisMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	
	private final static IntWritable one = new IntWritable(1);
	private final static int type_col = 4;
	private Text type = new Text();

@Override
protected void map(LongWritable keyE, Text valE, Context context) throws IOException,InterruptedException
	{
		// Ignoring header
		if (keyE.get()!=0) {
			String line = valE.toString();
			// Split and get the type 
			String[] splitted = line.split(";");
			String typeString = splitted[type_col];
			type.set(typeString);
			// Emit (type, 1)
			context.write(type, one);
		}
    }

public void run(Context context) throws IOException, InterruptedException {
    setup(context);
    while (context.nextKeyValue()) {
        map(context.getCurrentKey(), context.getCurrentValue(), context);
    }
    cleanup(context);
}

}






