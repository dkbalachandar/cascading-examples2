This is a simple program to remove the duplicate numbers in a text file using Cascading Api.

To run this program, follow the below steps,

1. Clone this repository

2. Then build the jar by running mvn package and copy it into hadoop installed directory[/usr/local/hadoop/userLib]. Provide appropriate file permissions

3. Then login as Hadoop user and go to Hadoop directory[/usr/local/hadoop]

4. Create a input text file with some sentences[inputFile.txt] and copy that file to hadoop by running the below command,

      bin/hadoop fs -copyFromLocal inputFile.txt /fc/inputFile.txt

5. Run the jar
    bin/hadoop jar userLib/cascading-examples2-1.0-jar-with-dependencices.jar /fc/inputFile.txt /fc/output
