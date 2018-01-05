package cs.bigdata.Lab2.question3;
import org.apache.hadoop.io.*;        
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

// To complete according to your problem
public class TreesParisMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
	
	private Text borough = new Text();
	private IntWritable year = new IntWritable();
	private final static int borough_col = 1;
	private final static int year_col = 5;

@Override
protected void map(LongWritable keyE, Text valE, Context context) throws IOException,InterruptedException
    {
		// Ignoring header
		if (keyE.get()!=0) {
			String line = valE.toString();
			String[] splitted = line.split(";");
			
			// Get year and verify that it is mentionned
			String year_string = splitted[year_col];
			if (!year_string.equals("")) {	
				try {
					// Get borough and year
					String borough_string = splitted[borough_col];
					borough.set(borough_string);
					int year_int = Integer.parseInt(year_string);	
					year.set(year_int);	
					
					// Emit (year, borough) ascending --> First line will be oldest tree
					context.write(year, borough);
				}
				catch(Exception e) {
					System.out.println(e.toString());
				}	
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






