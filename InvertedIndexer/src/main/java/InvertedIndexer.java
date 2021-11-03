
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.*;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;

public class InvertedIndexer {
    /** 自定义FileInputFormat **/
    public static class FileNameInputFormat extends FileInputFormat<Text, Text> {
        @Override
        public RecordReader<Text, Text> createRecordReader(InputSplit split,
                                                           TaskAttemptContext context) throws IOException, InterruptedException {
            FileNameRecordReader fnrr = new FileNameRecordReader();
            fnrr.initialize(split, context);
            return fnrr;
        }
    }

    /** 自定义RecordReader **/
    public static class FileNameRecordReader extends RecordReader<Text, Text> {
        String fileName;
        LineRecordReader lrr = new LineRecordReader();

        @Override
        public Text getCurrentKey() throws IOException, InterruptedException {
            return new Text(fileName);
        }

        @Override
        public Text getCurrentValue() throws IOException, InterruptedException {
            return lrr.getCurrentValue();
        }

        @Override
        public void initialize(InputSplit arg0, TaskAttemptContext arg1)
                throws IOException, InterruptedException {
            lrr.initialize(arg0, arg1);
            fileName = ((FileSplit) arg0).getPath().getName();
        }

        public void close() throws IOException {
            lrr.close();
        }

        public boolean nextKeyValue() throws IOException, InterruptedException {
            return lrr.nextKeyValue();
        }

        public float getProgress() throws IOException, InterruptedException {
            return lrr.getProgress();
        }
    }


    public static class InvertedIndexMapper extends
            Mapper<Text, Text, Text, IntWritable> {
        //        private Set<String> stopwords;
//        private Path[] localFiles;
//        private String pattern = "[^\\w]"; // 正则表达式，代表不是0-9, a-z, A-Z,的所有其它字
//
//        public void setup(Context context) throws IOException, InterruptedException {
//            stopwords = new TreeSet<String>();
//            Configuration conf = context.getConfiguration();
//            localFiles = DistributedCache.getLocalCacheFiles(conf); // 获得停词表
//            for (int i = 0; i < localFiles.length; i++) {
//                String line;
//                BufferedReader br =
//                        new BufferedReader(new FileReader(localFiles[i].toString()));
//                while ((line = br.readLine()) != null) {
//                    StringTokenizer itr = new StringTokenizer(line);
//                    while (itr.hasMoreTokens()) {
//                        stopwords.add(itr.nextToken());
//                    }
//                }
//            }
//        }
        private boolean caseSensitive; // 是否大小写敏感，从配置文件中读出赋值
        private Set<String> patternsToSkip = new HashSet<String>(); // 用来保存需过滤的关键词，从配置文件中读出赋值
        private Set<String> patternsToStop = new HashSet<String>(); // 用来保存需tingci的关键词，从配置文件中读出赋值
        private Configuration conf;
        private BufferedReader fis; // 保存文件输入流

        @Override
        public void setup(Context context) throws IOException,
                InterruptedException {
            conf = context.getConfiguration();

            caseSensitive = conf.getBoolean("wordcount.case.sensitive", true); // 配置文件中的wordcount.case.sensitive功能是否打开
            // wordcount.skip.patterns属性的值取决于命令行参数是否有-skip，具体逻辑在main方法中
            if (conf.getBoolean("wordcount.skip.patterns", false)) {

                URI[] patternsURIs = Job.getInstance(conf).getCacheFiles();


                Path patternsPath1 = new Path(patternsURIs[0].getPath());
                String patternsFileName1 = patternsPath1.getName().toString();
                parseSkipFile(patternsFileName1); // 将文件加入停词范围，具体逻辑参见parseStopFile(String
                // fileName)
                Path patternsPath2 = new Path(patternsURIs[1].getPath());
                String patternsFileName2 = patternsPath2.getName().toString();
                parseStopFile(patternsFileName2); // 将文件加入过滤范围，具体逻辑参见parseSkipFile(String
                // fileName)



            }
        }
        private void parseStopFile(String fileName) {
            try {
                fis = new BufferedReader(new FileReader(fileName));
//                System.out.println("fis"+ fis);
                String pattern = null;
                while ((pattern = fis.readLine()) != null) { // SkipFile的每一行都是一个需要过滤的pattern，例如\!
                    patternsToStop.add(pattern);
//                    System.out.println("pA"+ pattern);
                }
            } catch (IOException ioe) {
                System.err
                        .println("Caught exception while parsing the cached file '"
                                + StringUtils.stringifyException(ioe));
            }
        }


        private void parseSkipFile(String fileName) {
            try {
                fis = new BufferedReader(new FileReader(fileName));
//                System.out.println("fis"+ fis);
                String pattern = null;
                while ((pattern = fis.readLine()) != null) { // SkipFile的每一行都是一个需要过滤的pattern，例如\!
                    patternsToSkip.add(pattern);
//                    System.out.println("pA"+ pattern);
                }
            } catch (IOException ioe) {
                System.err
                        .println("Caught exception while parsing the cached file '"
                                + StringUtils.stringifyException(ioe));
            }
        }

        protected void map(Text key, Text value, Context context)
                throws IOException, InterruptedException {
            // map()函数这里使用自定义的FileNameRecordReader
            // 得到key: filename文件名; value: line_string每一行的内容
            String temp = new String();
            String line = value.toString().toLowerCase();
            for (String pattern : patternsToSkip) { // 将数据中所有满足patternsToSkip的pattern都过滤掉
                line = line.replaceAll(pattern, " ");
            }
            StringTokenizer itr = new StringTokenizer(line);
            while (itr.hasMoreTokens()) {
                String nextword = itr.nextToken();
                Text word = new Text();
                if(patternsToStop.contains(nextword)){
                    continue;
                }
                if(Pattern.compile("^[-\\+]?[\\d]*$").matcher(nextword).matches()) {
                    continue;
                }
                word.set(nextword);
                if(nextword.length() >= 3) {

                    word.set(word + "#" + key);
                    context.write(word, new IntWritable(1));
                }
            }
        }
    }

    /** 使用Combiner将Mapper的输出结果中value部分的词频进行统计 **/
    public static class SumCombiner extends
            Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    /** 自定义HashPartitioner，保证 <term, docid>格式的key值按照term分发给Reducer **/
    public static class NewPartitioner extends HashPartitioner<Text, IntWritable> {
        public int getPartition(Text key, IntWritable value, int numReduceTasks) {
            String term = new String();
            term = key.toString().split("#")[0]; // <term#docid>=>term
            return super.getPartition(new Text(term), value, numReduceTasks);
        }
    }












    public static class InvertedIndexReducer extends
            Reducer<Text, IntWritable, Text, Text> {
        private Text word1 = new Text();
        private Text word2 = new Text();
        String temp = new String();
        static Text CurrentItem = new Text(" ");
        static List<String> postingList = new ArrayList<String>();


        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            word1.set(key.toString().split("#")[0]);
            temp = key.toString().split("#")[1];
            for (IntWritable val : values) {
                sum += val.get();
            }
            word2.set("<" + sum + "," + temp + ">");
            if (!CurrentItem.equals(word1) && !CurrentItem.equals(" ")) {
                StringBuilder out = new StringBuilder();
                int str_length = 0;
                for (String p : postingList) {
                    str_length += 1;
                }
                for (int i = 0; i < str_length-1; i++){
                    for (int j = 0; j < str_length-1-i; j++){
                        String temp = "";
                        int sum_j = Integer.valueOf(postingList.get(j).substring(postingList.get(j).indexOf("<")+1, postingList.get(j).indexOf(",")));
                        int sum_k = Integer.valueOf(postingList.get(j+1).substring(postingList.get(j+1).indexOf("<")+1, postingList.get(j+1).indexOf(",")));
                        if (sum_j < sum_k) {
                            temp = postingList.get(j);
                            postingList.set(j, postingList.get(j+1));
                            postingList.set(j+1, temp);
                        }
                    }
                }
                long count = 0;
                for (String p : postingList) {
                    String endres = p.substring(p.indexOf(",")+1,p.indexOf(">"))+"#"+ p.substring(p.indexOf("<")+1,p.indexOf(","));
                    out.append(endres);
                    out.append("，");
                    count =
                            count
                                    + Long.parseLong(p.substring(p.indexOf("<")+1,p.indexOf(",")));
                }
                if (count > 0) {
                    String currentItem = CurrentItem + ":";
                    context.write(new Text(currentItem), new Text(out.toString()));
                }
                postingList = new ArrayList<String>();
            }
            CurrentItem = new Text(word1);
            postingList.add(word2.toString()); // 不断向postingList也就是文档名称中添加词表
        }

        // cleanup 一般情况默认为空，有了cleanup不会遗漏最后一个单词的情况

        public void cleanup(Context context) throws IOException,
                InterruptedException {
            StringBuilder out = new StringBuilder();
            int str_length = 0;
            for (String p : postingList) {
                str_length += 1;
            }

            for (int i = 0; i < str_length-1; i++){
                for (int j = 0; j < str_length-1-i; j++){
                    String temp = "";
                    int sum_j = Integer.valueOf(postingList.get(j).substring(postingList.get(j).indexOf("<")+1, postingList.get(j).indexOf(",")));
                    int sum_k = Integer.valueOf(postingList.get(j+1).substring(postingList.get(j+1).indexOf("<")+1, postingList.get(j+1).indexOf(",")));
                    if (sum_j < sum_k) {
                        temp = postingList.get(j);
                        postingList.set(j, postingList.get(j+1));
                        postingList.set(j+1, temp);
                    }
                }
            }
            long count = 0;
            for (String p : postingList) {
                String endres = p.substring(p.indexOf(",")+1,p.indexOf(">"))+"#"+ p.substring(p.indexOf("<")+1,p.indexOf(","));
                out.append(endres);
                out.append("，");
                count =
                        count
                                + Long.parseLong(p.substring(p.indexOf("<")+1,p.indexOf(",")));
            }
            if (count > 0) {
                String currentItem = CurrentItem + ":";
                context.write(new Text(currentItem), new Text(out.toString()));
            }
        }

    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "inverted index");
        GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
        String[] remainingArgs = optionParser.getRemainingArgs();
        List<String> otherArgs = new ArrayList<String>(); // 除了 -skip 以外的其它参数以外的其它参数
        for (int i = 0; i < remainingArgs.length; ++i) {

            if ("-skip".equals(remainingArgs[i])) {
                job.addCacheFile(new Path(remainingArgs[++i]).toUri()); // 将
                job.addCacheFile(new Path(remainingArgs[++i]).toUri());
                // 后面的参数，即skip模式文件的url，加入本地化缓存中
                job.getConfiguration().setBoolean("wordcount.skip.patterns",
                        true); // 这里设置的wordcount.skip.patterns属性，在mapper中使用
            } else {
                otherArgs.add(remainingArgs[i]); // 将除了 -skip
                // 以外的其它参数加入otherArgs中
            }
        }
        job.setJarByClass(InvertedIndexer.class);
        job.setInputFormatClass(FileNameInputFormat.class);
        job.setMapperClass(InvertedIndexMapper.class);
        job.setCombinerClass(SumCombiner.class);
        job.setReducerClass(InvertedIndexReducer.class);
        job.setPartitionerClass(NewPartitioner.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs.get(0)));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs.get(1)));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}