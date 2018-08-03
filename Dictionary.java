//package org.myorg;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Dictionary {

	public static class TokenizerMapper extends
			Mapper<Object, Text, Text, Text> {

		private final static Text one = new Text("1");
		private Text word = new Text();

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				context.write(word, one);
			}
		}
	}

	public static class DictionaryReducer extends
			Reducer<Text, Text, Text, Text> {

		public static WordFinder finderWord = new WordFinder(
				"hdfs://localhost:9000/words.txt",
				"hdfs://localhost:9000/dictionary.txt");

		Path ptWords = new Path("hdfs://localhost:9000/words.txt");
		Path ptDictionary = new Path("hdfs://localhost:9000/dictionary.txt");
		FileSystem fsWords;
		Configuration conf;
		public DictionaryReducer() throws IOException {
			conf = new Configuration();
			fsWords = FileSystem.get(conf);
		}

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			// Find the meaning for each key and write the meaning in the file
			Text writeText;
			if (!finderWord.verifyWord(key.toString().toUpperCase(), fsWords, ptWords)) {
				writeText = new Text("Word is not found.");
			} else {
				// Find the meaning of the word
				writeText = new Text(finderWord.findWordMeaning(key.toString().toUpperCase(), fsWords, ptDictionary));
			}
			context.write(key, writeText);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Dictionary");
		job.setJarByClass(Dictionary.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(DictionaryReducer.class);
		job.setReducerClass(DictionaryReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(
				"hdfs://localhost:9000/input100.txt"));
		FileOutputFormat.setOutputPath(job, new Path(
				"hdfs://localhost:9000/output100.txt"));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
