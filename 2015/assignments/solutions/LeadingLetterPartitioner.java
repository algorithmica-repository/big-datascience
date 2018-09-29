import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;

public class LeadingLetterPartitioner implements Partitioner<Text, Text> {
    @Override
    public int getPartition(Text key, Text value, int numPartitions) {
        return startsWithVowel(key.toString()) ? 0 : 1;
    }

    static boolean startsWithVowel(String word) {
        return "aeiou".contains(word.toLowerCase().substring(0, 1));
    }

    @Override
    public void configure(JobConf jc) {
    }
}
