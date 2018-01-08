package cs.bigdata.Lab2.question2;
import org.apache.hadoop.io.*;        
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class TreesParisMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {
	
	private FloatWritable height = new FloatWritable();
	private final static int type_col = 4;
	private final static int height_col = 6;	
	private Text type = new Text();

@Override
protected void map(LongWritable keyE, Text valE, Context context) throws IOException,InterruptedException
    {
		// Ignoring header
		if (keyE.get()!=0) {
			String line = valE.toString();
			String[] splitted = line.split(";");
			
			try {
				// Get type and height
				float height_float = Float.parseFloat(splitted[height_col]);
				height.set(height_float);
				String type_string = splitted[type_col];
				type.set(type_string);
				
				//Emit (type, height)
				context.write(type, height);
			}
			catch (Exception e) {
				System.out.println(e.toString());
			}
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






