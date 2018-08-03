Assuming Hadoop is already installed and configured.

1. Copy all the files to a folder
2. Compile the source file using:
	javac *.java
3. Create Dictionary.jar file using:
	jar cvfe dictionary.jar Fictionary *.class
	
4. Run the jar file in hadoop using:
	hadoop jar Dictionary.jar Dictionary
