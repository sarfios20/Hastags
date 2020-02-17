import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCounterMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	
	private final static IntWritable one = new IntWritable(1);
	private final static Pattern SPLIT_PATTERN = Pattern.compile("\\s+");

  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
	  System.out.println("inicio: ");
	  String line = value.toString();
	  line = line.replaceAll("\\\\n", "\n");
	  Text currentWord = new Text();
	  
	  String words[] = SPLIT_PATTERN.split(line);

	  for(int i = 0; i < words.length; i++){
		  System.out.println("word: " + words[i]);
		  if(!words[i].matches("#[a-zA-Z0-9]+")){
			  continue;
		  }
		  if(words[i].isEmpty()){
			  continue;
		  }
		  System.out.println("match: " + words[i]);
		  currentWord = new Text(words[i]);
		  context.write(currentWord, one);
	  }
	  
	  
  }
}
