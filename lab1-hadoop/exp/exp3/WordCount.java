import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class WordCount {
    public WordCount() {
    }

    public static void main(String[] args) throws Exception {
        // 设置 Hadoop Configuration
        Configuration conf = new Configuration();
        
        // 使用 GenericOptionsParser 获取命令行参数
        String[] otherArgs = (new GenericOptionsParser(conf, args)).getRemainingArgs();
        
        // 如果输入参数个数 <2 则返回错误提示
        if(otherArgs.length < 2) {
            System.err.println("Usage: wordcount <in> [<in>...] <out>");
            System.exit(2);
        }
        
        // 设置 Hadoop Job
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(WordCount.TokenizerMapper.class);
        job.setCombinerClass(WordCount.IntSumReducer.class);
        job.setReducerClass(WordCount.IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        // 添加输入文件
        for(int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        
        // 设置输出文件
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));
        
        // 提交任务并等待任务完成，如果成功则返回0，反之则返回1
        System.exit(job.waitForCompletion(true)?0:1);
    }
 
    // 定义 SumReducer 用于计算每个单词出现的总次数
    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();
 
        public IntSumReducer() {
        }
 
        public void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
            int sum = 0;
            
            // 遍历所有IntWritable，求和得到单词的总出现次数
            IntWritable val;
            for(Iterator i$ = values.iterator(); i$.hasNext(); sum += val.get()) {
                val = (IntWritable)i$.next();
            }
 
            // 写入结果
            this.result.set(sum);
            context.write(key, this.result);
        }
    }
 
    // 定义 TokenizerMapper 用于将每行文本切分为单词，并输出每个单词及其出现次数（在该行文本中）
    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
        private static final IntWritable one = new IntWritable(1);
        private Text word = new Text();
 
        public TokenizerMapper() {
        }
 
        public void map(Object key, Text value, Mapper<Object, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            
            // 当还有更多单词时，继续获取下一个单词并输出
            while(itr.hasMoreTokens()) {
                this.word.set(itr.nextToken());
                context.write(this.word, one);
            }
 
        }
    }
}
