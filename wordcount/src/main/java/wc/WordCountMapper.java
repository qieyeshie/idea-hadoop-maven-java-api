package wc;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

//泛型 k1 v1  k2 v2
public class WordCountMapper extends Mapper<LongWritable,Text,Text,IntWritable>{
    @Override
    protected void map(LongWritable key1, Text value1, Context context)
        throws IOException, InterruptedException {
        //数据
        String data = value1.toString();
        //分词
        String[] words = data.split(" ");
        //输出 k2  v2
        for (String w:words){
            context.write(new Text(w), new IntWritable(1));
        }
    }
}
