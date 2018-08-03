import java.io.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;

public class WordFinder {

	public static String WordFilePath = "";
	public static String DictionaryFilePath = "";

	public WordFinder(String wordFilePath, String dictionaryPath) {
		WordFilePath = wordFilePath;
		DictionaryFilePath = dictionaryPath;
	}

	public String runWordFinder(String inputWord, FileSystem fs, Path pt) throws IOException {
		// Check if input word is in word file
		if (!verifyWord(inputWord, fs, pt)) {
			return "Word is not found.";
		} else {
			// Find the meaning of the word
			return findWordMeaning(inputWord, fs, pt);
		}
	}
	
	public boolean verifyWord(String word, FileSystem fs, Path pt) throws IOException {
		boolean result = false;

		BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
		String line;
		while ((line = br.readLine()) != null) {
			if (line.toUpperCase().equals(word)) {
				result = true;
				break;
			}
		}
		br.close();
		//fs.close();
		return result;
	}

	public String findWordMeaning(String word, FileSystem fs, Path pt) throws IOException {
		// Read the dictionary file		
		BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
		
		String wordMeaning = "";
		String line;
		while ((line = br.readLine()) != null) {
			if (line.equals(word)) {
				// Until next word is hit print all the line
				while ((line = br.readLine()) != null) {
					if (!processLine(line, word))
						break;
					wordMeaning += " " +line;
				}
				break;
			}
		}
		br.close();
		//fs.close();
		return wordMeaning;
	}

	private boolean processLine(String line, String inputWord) {
		// Check if line has only single word
		String[] splitLine = line.split(" ");
		// If line's length is 1 that is single word and if it is uppercase then
		// next word is hit, so process it false.
		if (splitLine.length == 1 && line.matches("[A-Z]+") && !line.equals(inputWord))
			return false;
		return true;
	}

	public boolean addWord(String inputWord) {
		try {
			BufferedWriter writer = new BufferedWriter(new FileWriter(WordFilePath, true));
			writer.write("\n");
			writer.write(inputWord);
			writer.close();
			return true;
		} catch (IOException ioe) {
			System.err.format("IOException: %s%n", ioe);
			return false;
		}
	}
}
