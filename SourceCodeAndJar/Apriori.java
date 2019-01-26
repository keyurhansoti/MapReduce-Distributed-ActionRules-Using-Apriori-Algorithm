package org.wc;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Scanner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Apriori
{
  public static ArrayList<String> attributeList = new ArrayList();
  public static ArrayList<String> dataList = new ArrayList();
  public static ArrayList<String> originalValues = new ArrayList();
  public static ArrayList<String> stableValues = new ArrayList();
  public static ArrayList<String> decisionValues = new ArrayList();
  public static ArrayList<String> fromAndTo = new ArrayList();
  public static String dsisnFrom = new String();
  public static String dsisnTo = new String();
  public static int minimumSupport = 0;
  public static int minimumConfidence = 0;
  public static Scanner sc = new Scanner(System.in);
  public static int index = 0;
  
  public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException
  {
    File attribute = new File(args[0]);
    FileReader attribute_reader = new FileReader(attribute);
    BufferedReader attribute_buffer = new BufferedReader(attribute_reader);
    String att = new String();
    while ((att = attribute_buffer.readLine()) != null)
    {
    	attributeList.addAll(Arrays.asList(att.split("\\s+")));
    	originalValues.addAll(Arrays.asList(att.split("\\s+")));
    }
    int count = 0;
    attribute_reader.close();
    attribute_buffer.close();
    File data = new File(args[1]);
    FileReader data_reader = new FileReader(data);
    BufferedReader data_buffer = new BufferedReader(data_reader);
    String d = new String();
    while ((d = data_buffer.readLine()) != null) {
      count++;
    }
    data_reader.close();
    data_buffer.close();
    setStableValues();
    setDecisionValues();
    setFromAndTo(args[1]);
    System.out.println("Please enter minimum Support: ");
    minimumSupport = sc.nextInt();
    System.out.println("Please enter minimum Confidence %: ");
    minimumConfidence = sc.nextInt();
    sc.close();
    
    Configuration configuration = new Configuration();
    
    configuration.set("mapreduce.input.fileinputformat.split.mazsize", data.length() / 5L + "");
    configuration.set("mapreduce.input.fileinputformat.split.minsize", "0");
    
    configuration.setInt("count", count);
    configuration.setStrings("attributes", (String[])Arrays.copyOf(originalValues.toArray(), originalValues.toArray().length, String[].class));
    
    configuration.setStrings("stable", (String[])Arrays.copyOf(stableValues.toArray(), stableValues.toArray().length, String[].class));
    
    configuration.setStrings("decision", (String[])Arrays.copyOf(decisionValues.toArray(), decisionValues.toArray().length, String[].class));
    
    configuration.set("decisionFrom", dsisnFrom );
    configuration.set("decisionTo", dsisnTo );
    configuration.set("support",  minimumSupport + "" );
    configuration.set("confidence",  minimumConfidence + "");
    
    Job actionRulesJob = Job.getInstance(configuration);
    
    actionRulesJob.setJarByClass(ActionRules.class);
    
    actionRulesJob.setMapperClass(ActionRules.JobMapper.class);
    actionRulesJob.setReducerClass(ActionRules.JobReducer.class);
    
    actionRulesJob.setNumReduceTasks(1);
    
    actionRulesJob.setOutputKeyClass(Text.class);
    actionRulesJob.setOutputValueClass(Text.class);
    
    FileInputFormat.addInputPath(actionRulesJob, new Path(args[1]));
    
    FileOutputFormat.setOutputPath(actionRulesJob, new Path(args[2]));
    
    actionRulesJob.waitForCompletion(true);
    
    Job associationActionRulesJob = Job.getInstance(configuration);
    
    associationActionRulesJob.setJarByClass(AssociationActionRules.class);
    
    associationActionRulesJob.setMapperClass(AssociationActionRules.JobMapper.class);
    
    associationActionRulesJob.setReducerClass(AssociationActionRules.JobReducer.class);
    
    associationActionRulesJob.setNumReduceTasks(1);
    
    associationActionRulesJob.setOutputKeyClass(Text.class);
    associationActionRulesJob.setOutputValueClass(Text.class);
    
    FileInputFormat.addInputPath(associationActionRulesJob, new Path(args[1]));
    
    FileOutputFormat.setOutputPath(associationActionRulesJob, new Path(args[3]));
    
    associationActionRulesJob.waitForCompletion(true);
  }
  
  public static void setStableValues()
  {
    boolean flag = false;
    String[] stable = null;
    System.out.println("-------------Apriori  Algorithm------------");
    System.out.println("Available attributes are: " + attributeList.toString());
    
    System.out.println("Please enter the Stable Attribute(s):");
    String s = sc.next();
    if (s.split(",").length > 1)
    {
      stable = s.split(",");
      for (int j = 0; j < stable.length; j++) {
        if (!attributeList.contains(stable[j]))
        {
          System.out.println("Invalid Stable attribute(s)");
          flag = true;
          break;
        }
      }
      if (!flag)
      {
    	  stableValues.addAll(Arrays.asList(stable));
        attributeList.removeAll(stableValues);
      }
    }
    else if (!attributeList.contains(s))
    {
      System.out.println("Invalid Stable attribute(s)");
    }
    else
    {
    	stableValues.add(s);
      attributeList.removeAll(stableValues);
    }
    System.out.println("Stable Attribute(s): " + stableValues.toString());
    
    System.out.println("Available Attribute(s): " + attributeList.toString());
  }
  
  public static void setDecisionValues()
  {
    System.out.println("1. Enter the Decision Attribute:");
    String s = sc.next();
    if (!attributeList.contains(s))
    {
      System.out.println("Invalid Decision attribute(s)");
    }
    else
    {
    	decisionValues.add(s);
      index = originalValues.indexOf(s);
      attributeList.removeAll(decisionValues);
    }
  }
  
  public static void setFromAndTo(String args)
  {
    HashSet<String> set = new HashSet();
    File data = new File(args);
    try
    {
      FileReader fileReader = new FileReader(data);
      BufferedReader data_buffer = new BufferedReader(fileReader);
      String str = new String();
      while ((str = data_buffer.readLine()) != null) {
        set.add(str.split(",")[index]);
      }
      fileReader.close();
      data_buffer.close();
    }
    catch (IOException e)
    {
      e.printStackTrace();
    }
    System.out.println();
    Iterator<String> iterator = set.iterator();
    while (iterator.hasNext()) {
    	fromAndTo.add((String)originalValues.get(index) + (String)iterator.next());
    }
    System.out.println("Available decision attributes are: " + fromAndTo.toString());
    
    System.out.println("Enter decision FROM attribute: ");
    dsisnFrom = sc.next();
    System.out.println("Enter decision TO Attribute: ");
    dsisnTo = sc.next();
    System.out.println("Stable attributes are: " + stableValues.toString());
    
    System.out.println("Decision attribute is: " + decisionValues.toString());
    
    System.out.println("Decision FROM : " + dsisnFrom);
    System.out.println("Decision TO : " + dsisnTo);
    System.out.println("Flexible Attribute(s) are: " + attributeList.toString());
  }
}
