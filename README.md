--------------------------------------------------------------------------------------------------------------
						MapReduce-Distributed-ActionRules-Using-Apriori-Algorithm
--------------------------------------------------------------------------------------------------------------

Execution Steps:

1. Compiling the code and prepare the jar file
	a. From your local machine, Open Eclipse
	b. Click on File -> New -> Other -> Maven -> Maven Project and select Next
	c. Select "Create a simple project" checkbox and select Next
	d. Give a GroupID(package name) as "org.wc"
	e. Give ArtifactID(project name) as "Part3Group6" and select Finish
	f. Open the pom.xml file inside the Part3Group6 project
	g. After the <version> tag, copy and paste following dependencies:
		<dependencies>

		  <dependency>
    		    <groupId>org.apache.hadoop</groupId>
		    <artifactId>hadoop-common</artifactId>
		    <version>2.8.1</version>
		  </dependency>

		  <dependency>
		    <groupId>org.apache.hadoop</groupId>
		    <artifactId>hadoop-mapreduce-client-core</artifactId>
		    <version>2.8.1</version>
		  </dependency>

		  <dependency>
		    <groupId>org.apache.hadoop</groupId>
		    <artifactId>hadoop-yarn-common</artifactId>
		    <version>2.8.1</version>
		  </dependency>

		  <dependency>
		    <groupId>org.apache.hadoop</groupId>
		    <artifactId>hadoop-mapreduce-client-common</artifactId>
		    <version>2.8.1</version>
		  </dependency>

		</dependencies>

		All the above dependencies can be downloaded from: https://mvnrepository.com/artifact/org.apache.hadoop
	f. Save the pom.xml
	g. Right click on src/main/java and select New -> Package. Give the package name as org.wc(the name that we gave for GroupID) and select Finish
	h. From the submission, Download the files "ActionRules.java", "Apriori.java", "AssociationActionRules.java" and paste in the given package.
	f. Run the maven commands (generate-resources, install and build) on the project.
	i. After maven commands are executed, inside target folder (in workspace) we can find the project jar file. Rename it as "Apriori.jar"
2. Using Winscp, copy the jar file, mamographic and car datasets into dsba cluster.
3. In cloudera, login to dsba cluster ("ssh -X dsba-hadoop.uncc.edu -l <username>") and put the files in hadoop using the following commands
	hadoop fs -put Aprori.jar
	hadoop fs -put Mammographic-Input
	hadoop fs -put Car-Input
4. Execute the jar file using the command
	hadoop jar Apriori.jar org.wc.Apriori Mammographic/mammographic_attribute.txt Mammographic/mammographic_masses.data.txt KDD/ActionRules_Output KDD/Output
5. To get the files:
	hadoop fs -get KDD/Output/part-r-00000  /users/aamula/MammographicAssociationActionRulesOutput.txt
	hadoop fs -get KDD/ActionRules_Output/part-r-00000  /users/aamula/MammographicActionRulesOutput.txt
6. To Delete a folder
	hadoop fs -rm -r KDD/ActionRules_Output
	hadoop fs -rm -r KDD/Output
7. Using Winscp, we can get the output from cloudera to local machine.
 
