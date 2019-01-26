package org.wc;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class ActionRulesClass
{
  public static class JobMapperClass
    extends Mapper<LongWritable, Text, Text, Text>
  {
    int noOfLines = 0;
    boolean falseParametersValue;
    double minimumSupportValue;
    double minimumConfidenceValue;
    String decisionAttributeValue;
    String decisionFrom;
    String decisionTo;
    public static ArrayList<String> listOfAttributeNames;
    public static ArrayList<String> listOfStableAttributes;
    public static ArrayList<String> listOfStableAttributeValues;
    public static ArrayList<ArrayList<String>> arrayListOfActionRules;
    static Map<ArrayList<String>, Integer> dataValues;
    static Map<String, HashSet<String>> valuesOfDistinctAttributes;
    static Map<String, HashSet<String>> decisionValues;
    static Map<HashSet<String>, HashSet<String>> attributeValues;
    static Map<HashSet<String>, HashSet<String>> valuesOfReducedAttributes;
    static Map<ArrayList<String>, HashSet<String>> markedValues;
    static Map<ArrayList<String>, HashSet<String>> possibleRulesData;
    static Map<ArrayList<String>, String> certainRulesData;
    
    public JobMapperClass()
    {
      this.falseParametersValue = false;
      
      dataValues = new HashMap<ArrayList<String>, Integer>();
      arrayListOfActionRules = new ArrayList();
      listOfAttributeNames = new ArrayList();
      listOfStableAttributes = new ArrayList();
      listOfStableAttributeValues = new ArrayList();
      
      attributeValues = new HashMap<HashSet<String>, HashSet<String>>();
      valuesOfReducedAttributes = new HashMap<HashSet<String>, HashSet<String>>();
      valuesOfDistinctAttributes = new HashMap<String, HashSet<String>>();
      decisionValues = new HashMap<String, HashSet<String>>();
      certainRulesData = new HashMap<ArrayList<String>, String>();
      markedValues = new HashMap<ArrayList<String>, HashSet<String>>();
      possibleRulesData = new HashMap<ArrayList<String>, HashSet<String>>();
    }
    
    protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context)
      throws IOException, InterruptedException
    {
      listOfAttributeNames = new ArrayList(Arrays.asList(context.getConfiguration().getStrings("attributes")));
      
      listOfStableAttributes = new ArrayList(Arrays.asList(context.getConfiguration().getStrings("stable")));
      
      this.decisionAttributeValue = context.getConfiguration().getStrings("decision")[0];
      
      this.decisionFrom = context.getConfiguration().get("decisionFrom");
      this.decisionTo = context.getConfiguration().get("decisionTo");
      this.minimumSupportValue = Double.parseDouble(context.getConfiguration().get("support"));
      
      this.minimumConfidenceValue = Double.parseDouble(context.getConfiguration().get("confidence"));
      
      super.setup(context);
    }
    
    
    private void splitDataFunction(Text inputValue, int lineCount)
    {
      int lineNumber = lineCount;
      
      String inputData = inputValue.toString();
      
      ArrayList<String> lineData = new ArrayList(Arrays.asList(inputData.split("\t|,")));
      if (!emptyValueCheckInStringArray(lineData))
      {
        lineNumber++;
        
        ArrayList<String> temporaryList = new ArrayList<String>();
        for (int j = 0; j < lineData.size(); j++)
        {
          String currentAttributeValue = (String)lineData.get(j);
          String nameOfAttribute = (String)listOfAttributeNames.get(j);
          String keyValue = nameOfAttribute + currentAttributeValue;
          
          temporaryList.add(keyValue);
          HashSet<String> setData;
          if (valuesOfDistinctAttributes.containsKey(nameOfAttribute)) {
            setData = (HashSet)valuesOfDistinctAttributes.get(nameOfAttribute);
          } else {
            setData = new HashSet<String>();
          }
          setData.add(keyValue);
          valuesOfDistinctAttributes.put(nameOfAttribute, setData);
        }
        if (!dataValues.containsKey(temporaryList))
        {
          dataValues.put(temporaryList, Integer.valueOf(1));
          for (String listKey : temporaryList)
          {
            HashSet<String> mapKey = new HashSet<String>();
            mapKey.add(listKey);
            settingMapFunction(attributeValues, mapKey, lineNumber);
          }
        }
        else
        {
          dataValues.put(temporaryList, Integer.valueOf(((Integer)dataValues.get(temporaryList)).intValue() + 1));
        }
      }
    }
    
    private static boolean emptyValueCheckInStringArray(ArrayList<String> lineData)
    {
      return lineData.contains("");
    }
    
    private static void settingMapFunction(Map<HashSet<String>, HashSet<String>> values, HashSet<String> keyValue, int lineNumber)
    {
      HashSet<String> temporarySet = new HashSet<String>();
      if (values.containsKey(keyValue)) {
        temporarySet.addAll((Collection)values.get(keyValue));
      }
      temporarySet.add("x" + lineNumber);
      values.put(keyValue, temporarySet);
    }
    
    private void assignDecisionAttributeValues()
    {
      HashSet<String> distinctDecisionValues = (HashSet)valuesOfDistinctAttributes.get(this.decisionAttributeValue);
      for (String data : distinctDecisionValues)
      {
        HashSet<String> newHashValue = new HashSet<String>();
        HashSet<String> ultimateHashValue = new HashSet<String>();
        newHashValue.add(data);
        if (decisionValues.containsKey(data)) {
          ultimateHashValue.addAll((Collection)decisionValues.get(data));
        }
        if (attributeValues.containsKey(newHashValue)) {
          ultimateHashValue.addAll((Collection)attributeValues.get(newHashValue));
        }
        decisionValues.put(data, ultimateHashValue);
        attributeValues.remove(newHashValue);
      }
    }
    
    protected void map(LongWritable keyValue, Text inputValue, Mapper<LongWritable, Text, Text, Text>.Context context)
      throws IOException, InterruptedException
    {
      splitDataFunction(inputValue, this.noOfLines);
      for (int i = 0; i < listOfStableAttributes.size(); i++)
      {
        HashSet<String> distinctStableValues = (HashSet<String>)valuesOfDistinctAttributes.get(listOfStableAttributes.get(i));
        for (String string : distinctStableValues) {
          if (!listOfStableAttributeValues.contains(string)) {
            listOfStableAttributeValues.add(string);
          }
        }
      }
      assignDecisionAttributeValues();
      
      this.noOfLines += 1;
    }


    
    private void performLERSFunction()
    {
      int loopCount = 0;
      while (!attributeValues.isEmpty())
      {
        for (Map.Entry<HashSet<String>, HashSet<String>> setvalue : attributeValues.entrySet())
        {
          ArrayList<String> setKeyValue = new ArrayList<String>();
          setKeyValue.addAll((Collection)setvalue.getKey());
          
          HashSet<String> setValue = (HashSet)setvalue.getValue();
          if (!((HashSet)setvalue.getValue()).isEmpty())
          {
            for (Map.Entry<String, HashSet<String>> decisionSetValue : decisionValues.entrySet()) {
              if (((HashSet)decisionSetValue.getValue()).containsAll((Collection)setvalue.getValue()))
              {
                certainRulesData.put(setKeyValue, decisionSetValue.getKey());
                markedValues.put(setKeyValue, setvalue.getValue());
                break;
              }
            }
            if (!markedValues.containsKey(setKeyValue))
            {
              HashSet<String> possibleRulesValuesSet = new HashSet<String>();
              for (Map.Entry<String, HashSet<String>> decisionSetValue : decisionValues.entrySet()) {
                possibleRulesValuesSet.add(decisionSetValue.getKey());
              }
              if (possibleRulesValuesSet.size() > 0) {
                possibleRulesData.put(setKeyValue, possibleRulesValuesSet);
              }
            }
          }
        }
        deleteMarkedValuesFunction();
        
        combiningPossibleRulesFunction();
      }
    }
    
    
    protected void cleanup(Mapper<LongWritable, Text, Text, Text>.Context context)
      throws IOException, InterruptedException
    {
      performLERSFunction();
      
      generateActionRules(context);
      
      super.cleanup(context);
    }
    
    static int calculationOfSupportLERS(ArrayList<String> keyValue, String value)
    {
      ArrayList<String> temporaryList = new ArrayList<String>();
      for (String value1 : keyValue) {
        if (!value1.equals("")) {
          temporaryList.add(value1);
        }
      }
      if (!value.equals("")) {
        temporaryList.add(value);
      }
      return findingLERSSupport(temporaryList);
    }
    
    private static int findingLERSSupport(ArrayList<String> temporaryList)
    {
      int countValue = 0;
      for (Map.Entry<ArrayList<String>, Integer> entry : dataValues.entrySet()) {
        if (((ArrayList)entry.getKey()).containsAll(temporaryList)) {
          countValue += ((Integer)entry.getValue()).intValue();
        }
      }
      return countValue;
    }
    
    static String LERSConfidenceCalculation(ArrayList<String> keyValue, String value)
    {
      int numerator = calculationOfSupportLERS(keyValue, value);
      int denominator = calculationOfSupportLERS(keyValue, "");
      int confidenceValue = 0;
      if (denominator != 0) {
        confidenceValue = numerator * 100 / denominator;
      }
      return String.valueOf(confidenceValue);
    }
    
    private static void deleteMarkedValuesFunction()
    {
      for (Map.Entry<ArrayList<String>, HashSet<String>> markedSet : markedValues.entrySet()) {
        attributeValues.remove(new HashSet((Collection)markedSet.getKey()));
      }
    }
    
    private static void combiningPossibleRulesFunction()
    {
      Set<ArrayList<String>> keySetValue = possibleRulesData.keySet();
      ArrayList<ArrayList<String>> keyListValues = new ArrayList();
      keyListValues.addAll(keySetValue);
      for (int i = 0; i < possibleRulesData.size(); i++) {
        for (int j = i + 1; j < possibleRulesData.size(); j++)
        {
          HashSet<String> valueOfCombineKeys = new HashSet((Collection)keyListValues.get(i));
          
          valueOfCombineKeys.addAll(new HashSet((Collection)keyListValues.get(j)));
          if (!checkSameGroup(valueOfCombineKeys)) {
            combineAttributeValues(valueOfCombineKeys);
          }
        }
      }
      removeRedundantValues();
      clearAttributeValues();
      possibleRulesData.clear();
    }
    
    
    private static void combineAttributeValues(HashSet<String> combinedKeys)
    {
      HashSet<String> combinedValues = new HashSet<String>();
      for (Map.Entry<HashSet<String>, HashSet<String>> attributeValue : attributeValues.entrySet()) {
        if (combinedKeys.containsAll((Collection)attributeValue.getKey())) {
          if (combinedValues.isEmpty()) {
            combinedValues.addAll((Collection)attributeValue.getValue());
          } else {
            combinedValues.retainAll((Collection)attributeValue.getValue());
          }
        }
      }
      if (combinedValues.size() != 0) {
        valuesOfReducedAttributes.put(combinedKeys, combinedValues);
      }
    }
    
    private static void removeRedundantValues()
    {
	Map.Entry<HashSet<String>, HashSet<String>> reducedAttributeValue;
      HashSet<String> mark = new HashSet<String>();
      for (Iterator i$ = valuesOfReducedAttributes.entrySet().iterator(); i$.hasNext();)
      {
        reducedAttributeValue = (Map.Entry)i$.next();
        for (Map.Entry<HashSet<String>, HashSet<String>> attributeValue : attributeValues.entrySet()) {
          if ((((HashSet)attributeValue.getValue()).containsAll((Collection)reducedAttributeValue.getValue())) || (((HashSet)reducedAttributeValue.getValue()).isEmpty())) {
            mark.addAll((Collection)reducedAttributeValue.getKey());
          }
        }
      }
      
      valuesOfReducedAttributes.remove(mark);
    }
    

    
    private static void clearAttributeValues()
    {
      attributeValues.clear();
      for (Map.Entry<HashSet<String>, HashSet<String>> reducedAttributeValue : valuesOfReducedAttributes.entrySet()) {
        attributeValues.put(reducedAttributeValue.getKey(), reducedAttributeValue.getValue());
      }
      valuesOfReducedAttributes.clear();
    }
    
    public ArrayList<String> generateActionRules(Mapper<LongWritable, Text, Text, Text>.Context context)
      throws IOException, InterruptedException
    {
      ArrayList<String> actions = null;
	Map.Entry<ArrayList<String>, String> certainRules1;
      
      String rule = "";
      ArrayList<String> rules = new ArrayList<String>();
      for (Iterator i$ = certainRulesData.entrySet().iterator(); i$.hasNext();)
      {
        certainRules1 = (Map.Entry)i$.next();
        
        String certainRules1Value = (String)certainRules1.getValue();
        if (certainRules1Value.equals(this.decisionFrom)) {
          for (Map.Entry<ArrayList<String>, String> certainRules2 : certainRulesData.entrySet())
          {
            ArrayList<String> certainRules1Key = (ArrayList<String>)certainRules1.getKey();
            
            ArrayList<String> certainRules2Key = (ArrayList<String>)certainRules2.getKey();
            if ((!certainRules1Key.equals(certainRules2Key)) && (((String)certainRules2.getValue()).equals(this.decisionTo)) && (stableAttributesValueCheckFunction(certainRules1Key, certainRules2Key)))
            {
              String primeAttribute = "";
              if (checkRulesSubSetFunction(certainRules1Key, certainRules2Key))
              {
                ArrayList<String> checkCertainValues1 = (ArrayList)certainRules1.getKey();
                
                ArrayList<String> tempList = new ArrayList<String>();
                
                rule = "";
                ArrayList<String> actionFrom = new ArrayList<String>();
                ArrayList<String> actionTo = new ArrayList<String>();
                actions = new ArrayList<String>();
		String value1;
                for (Iterator i2$ = checkCertainValues1.iterator(); i2$.hasNext();)
                {
                  value1 = (String)i2$.next();
                  if (listOfStableAttributeValues.contains(value1))
                  {
                    if (!actionTo.contains(value1))
                    {
                      rule = ruleFormation(rule, value1, value1);
                      
                      actionFrom.add(value1);
                      actionTo.add(value1);
                      actions.add(getAction(value1, value1));
                    }
                  }
                  else
                  {
                    primeAttribute = retrieveAttributeName(value1);
                    
                    ArrayList<String> checkCertainValues2 = (ArrayList<String>)certainRules2.getKey();
                    for (String value2 : checkCertainValues2) {
                      if (listOfStableAttributeValues.contains(value2))
                      {
                        if (!actionTo.contains(value2))
                        {
                          rule = ruleFormation(rule, value2, value2);
                          
                          actionFrom.add(value2);
                          actionTo.add(value2);
                          actions.add(getAction(value2, value2));
                        }
                      }
                      else if (!retrieveAttributeName(value2).equals(primeAttribute))
                      {
                        tempList.add(value2);
                      }
                      else if ((retrieveAttributeName(value2).equals(primeAttribute)) && (!actionTo.contains(value2)))
                      {
                        rule = ruleFormation(rule, value1, value2);
                        
                        actionFrom.add(value1);
                        actionTo.add(value2);
                        actions.add(getAction(value1, value2));
                      }
                    }
                  }
                }
                
                for (String missedValues : tempList) {
                  if (!actionTo.contains(missedValues))
                  {
                    rule = ruleFormation(rule, "", missedValues);
                    
                    actionFrom.add("");
                    actionTo.add(missedValues);
                    actions.add(getAction("", missedValues));
                  }
                }
                displayActionRule(actionFrom, actionTo, actions, rule, context);
                
                displayExtraRules(actionFrom, actionTo, context);
              }
            }
          }
        }
      }
      
      return rules;
    }
    
    private static boolean stableAttributesValueCheckFunction(ArrayList<String> keyValue, ArrayList<String> keyValue2)
      throws IOException, InterruptedException
    {
      List<String> stableAttributesList1 = new ArrayList<String>();
      List<String> stableAttributesList2 = new ArrayList<String>();
      for (String value : keyValue) {
        if (listOfStableAttributeValues.contains(value)) {
          stableAttributesList1.add(value);
        }
      }
      for (String value : keyValue2) {
        if (listOfStableAttributeValues.contains(value)) {
          stableAttributesList2.add(value);
        }
      }
      if (stableAttributesList2.containsAll(stableAttributesList1)) {
        return true;
      }
      return false;
    }
    
    private boolean checkRulesSubSetFunction(ArrayList<String> arrayListCertainRulesValue1, ArrayList<String> arrayListCertainRulesValue2)
    {
      ArrayList<String> arrayListPrimeAttributes1 = new ArrayList<String>();
      ArrayList<String> arrayListPrimeAttributes2 = new ArrayList<String>();
      for (String value : arrayListCertainRulesValue1)
      {
        String nameOfAttribute = retrieveAttributeName(value);
        if (!isStable(nameOfAttribute)) {
          arrayListPrimeAttributes1.add(nameOfAttribute);
        }
      }
      for (String value : arrayListCertainRulesValue2)
      {
        String nameOfAttribute = retrieveAttributeName(value);
        if (!isStable(nameOfAttribute)) {
          arrayListPrimeAttributes2.add(nameOfAttribute);
        }
      }
      if (arrayListPrimeAttributes2.containsAll(arrayListPrimeAttributes1)) {
        return true;
      }
      return false;
    }
    
    public static String retrieveAttributeName(String value)
    {
      for (Map.Entry<String, HashSet<String>> entryValue : valuesOfDistinctAttributes.entrySet()) {
        if (((HashSet)entryValue.getValue()).contains(value)) {
          return (String)entryValue.getKey();
        }
      }
      return null;
    }
    
    private static String ruleFormation(String rule, String value1, String value2)
    {
      if (!rule.isEmpty()) {
        rule = rule + "^";
      }
      rule = rule + "(" + retrieveAttributeName(value2) + "," + getAction(value1, value2) + ")";
      
      return rule;
    }
    
    private static String getAction(String leftSideValue, String rightSideValue)
    {
      return leftSideValue + " -> " + rightSideValue;
    }
    
    private void displayActionRule(ArrayList<String> actionFrom, ArrayList<String> actionTo, ArrayList<String> arrayListActionRules, String rule, Mapper<LongWritable, Text, Text, Text>.Context context)
      throws IOException, InterruptedException
    {
      int supportValue= calculationOfSupportLERS(actionTo, this.decisionTo);
      if (supportValue != 0)
      {
        String valueOfOldConfidence = String.valueOf(Integer.parseInt(LERSConfidenceCalculation(actionFrom, this.decisionFrom)) * Integer.parseInt(LERSConfidenceCalculation(actionTo, this.decisionTo)) / 100);
        
        String valueOfNewConfidnce = LERSConfidenceCalculation(actionTo, this.decisionTo);
        if ((supportValue >= this.minimumSupportValue) && (Double.parseDouble(valueOfOldConfidence) >= this.minimumConfidenceValue) && (!valueOfOldConfidence.equals("0")) && (!valueOfNewConfidnce.equals("0"))) {
          if (arrayListActionRules != null)
          {
            Collections.sort(arrayListActionRules);
            if (!arrayListOfActionRules.contains(arrayListActionRules))
            {
              arrayListOfActionRules.add(arrayListActionRules);
              try
              {
                Text key1 = new Text(rule + " ==> " + "(" + this.decisionAttributeValue + "," + this.decisionFrom + " -> " + this.decisionTo + ")");
                
                Text value1 = new Text(supportValue + ":" + valueOfNewConfidnce);
                
                context.write(key1, value1);
              }
              catch (IOException e)
              {
                e.printStackTrace();
              }
              catch (InterruptedException e)
              {
                e.printStackTrace();
              }
            }
          }
        }
      }
    }
    
    private void displayExtraRules(ArrayList<String> actionFrom, ArrayList<String> actionTo, Mapper<LongWritable, Text, Text, Text>.Context context)
      throws IOException, InterruptedException
    {
      ArrayList<String> arraylistStableValues = getStableValues(actionTo);
      ArrayList<String> arraylistAttributeValues = getAttributeValues(arraylistStableValues, this.decisionFrom, actionFrom);
      
      ArrayList<String> toBeAddedAttributes = acceptNewAttributeValues(actionFrom, actionTo, arraylistStableValues);
      for (String attributeValue : toBeAddedAttributes)
      {
        ArrayList<String> temporaryAttributeValues = new ArrayList<String>();
        temporaryAttributeValues.add(attributeValue);
        
        ArrayList<String> arraylistCheckList = getAttributeValues(arraylistStableValues, "", temporaryAttributeValues);
        if ((arraylistAttributeValues.containsAll(arraylistCheckList)) && (!arraylistCheckList.isEmpty()))
        {
          String subsetRule = new String();
          ArrayList<String> subsetActionFrom = new ArrayList<String>();
          ArrayList<String> subsetActionTo = new ArrayList<String>();
          ArrayList<String> subsetActions = new ArrayList<String>();
          if (isStable(retrieveAttributeName(attributeValue)))
          {
            subsetActionFrom.addAll(actionFrom);
            subsetActionFrom.add(attributeValue);
            
            subsetActionTo.addAll(actionTo);
            subsetActionTo.add(attributeValue);
          }
          else
          {
            subsetActionFrom = getSubActionFrom(actionFrom, actionTo, attributeValue);
            
            subsetActionTo.addAll(actionTo);
          }
          subsetRule = retrieveSubRules(subsetActionFrom, subsetActionTo, subsetActions);
          try
          {
            displayActionRule(subsetActionFrom, subsetActionTo, subsetActions, subsetRule, context);
          }
          catch (IOException e)
          {
            e.printStackTrace();
          }
          catch (InterruptedException e)
          {
            e.printStackTrace();
          }
        }
      }
    }
    private ArrayList<String> getSubActionFrom(ArrayList<String> actionFrom, ArrayList<String> actionTo, String alternateActionFrom)
    {
      ArrayList<String> lastActionFrom = new ArrayList<String>();
      for (int i = 0; i < actionTo.size(); i++)
      {
        HashSet<String> checkSameSet = new HashSet<String>();
        
        checkSameSet.add(alternateActionFrom);
        checkSameSet.add(actionTo.get(i));
        if (checkSameGroup(checkSameSet)) {
        	lastActionFrom.add(alternateActionFrom);
        } else if (i < actionFrom.size()) {
        	lastActionFrom.add(actionFrom.get(i));
        }
      }
      return lastActionFrom;
    }
    
    public static boolean checkSameGroup(HashSet<String> combinedKeys)
    {
      ArrayList<String> combinedKeyAttributes = new ArrayList<String>();
      Map.Entry<String, HashSet<String>> singleAttribute;
      for (Iterator i$ = valuesOfDistinctAttributes.entrySet().iterator(); i$.hasNext();)
      {
        singleAttribute = (Map.Entry)i$.next();
        for (String key : combinedKeys) {
          if (((HashSet)singleAttribute.getValue()).contains(key)) {
            if (!combinedKeyAttributes.contains(singleAttribute.getKey())) {
              combinedKeyAttributes.add(singleAttribute.getKey());
            } else {
              return true;
            }
          }
        }
      }
      
      return false;
    }

    private ArrayList<String> acceptNewAttributeValues(ArrayList<String> actionFrom, ArrayList<String> actionTo, ArrayList<String> stableValues)
    {
      ArrayList<String> arraylistStableAttributes = new ArrayList<String>();
      ArrayList<String> arraylistFlexibleAttributes = new ArrayList<String>();
      ArrayList<String> arraylistNewAttributes = new ArrayList<String>();
      for (String string : stableValues) {
        arraylistStableAttributes.add(retrieveAttributeName(string));
      }
      for (String string : actionTo) {
    	  arraylistFlexibleAttributes.add(retrieveAttributeName(string));
      }
      for (Map.Entry<String, HashSet<String>> mapValue : valuesOfDistinctAttributes.entrySet())
      {
        String mapKeyValue = (String)mapValue.getKey();
        HashSet<String> mapValues = (HashSet)mapValue.getValue();
        if ((!mapKeyValue.equals(this.decisionAttributeValue)) && ((arraylistStableAttributes.size() == 0) || (!arraylistStableAttributes.contains(mapKeyValue
        		)))) {
          if (isStable(mapKeyValue)) {
        	  arraylistNewAttributes.addAll(mapValues);
          } else if ((!arraylistFlexibleAttributes.isEmpty()) && (arraylistFlexibleAttributes.contains(mapKeyValue))) {
            for (String setValue : mapValues) {
              if ((!actionFrom.contains(setValue)) && (!actionTo.contains(setValue))) {
            	  arraylistNewAttributes.add(setValue);
              }
            }
          }
        }
      }
      return arraylistNewAttributes;
    }
    
    private ArrayList<String> getStableValues(ArrayList<String> actionFrom)
    {
      ArrayList<String> stableValues = listOfStableAttributeValues;
      ArrayList<String> toBeAdded = new ArrayList<String>();
      for (String value : actionFrom) {
        if (stableValues.contains(value)) {
          toBeAdded.add(value);
        }
      }
      return toBeAdded;
    }
    
    private ArrayList<String> getAttributeValues(ArrayList<String> stableValues, String decisionFrom, ArrayList<String> actionFrom)
    {
      ArrayList<String> temporary = new ArrayList<String>();
      ArrayList<String> attributeValues = new ArrayList<String>();
      int countOfLine = 0;
      
      temporary.addAll(stableValues);
      for (String from : actionFrom) {
        if (!from.equals("")) {
        	temporary.add(from);
        }
      }
      if (!decisionFrom.equals("")) {
    	  temporary.add(decisionFrom);
      }
      for (Map.Entry<ArrayList<String>, Integer> data1 : dataValues.entrySet())
      {
    	  countOfLine++;
        if (((ArrayList)data1.getKey()).containsAll(temporary)) {
          attributeValues.add("x" + countOfLine);
        }
      }
      return attributeValues;
    }
    
    private String retrieveSubRules(ArrayList<String> subActionFrom, ArrayList<String> subActionTo, ArrayList<String> subActions)
    {
      String ruleValue = "";
      for (int i = 0; i < subActionFrom.size(); i++)
      {
        ruleValue = ruleFormation(ruleValue, (String)subActionFrom.get(i), (String)subActionTo.get(i));
        subActions.add(getAction((String)subActionFrom.get(i), (String)subActionTo.get(i)));
      }
      return ruleValue;
    }
    
    public boolean isStable(String value)
    {
      if (listOfStableAttributeValues.containsAll((Collection)valuesOfDistinctAttributes.get(value))) {
        return true;
      }
      return false;
    }
  }
    

    

  
  public static class JobReducerClass
    extends Reducer<Text, Text, Text, Text>
  {
    protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
      throws IOException, InterruptedException
    {
      double supportValue = 0.0D;double confidenceValue = 0.0D;
      DecimalFormat dfValue = new DecimalFormat("###.##");
      int mapperValue = Integer.parseInt(context.getConfiguration().get("mapred.map.tasks"));
      
      int minPresence = mapperValue / 2;
      int total = 0;
      for (Text value : values)
      {
        total++;
        supportValue += Double.valueOf(dfValue.format(Double.parseDouble(value.toString().split(":")[0]))).doubleValue();
        
        confidenceValue += Double.valueOf(dfValue.format(Double.parseDouble(value.toString().split(":")[1]))).doubleValue();
      }
      if (total >= minPresence) {
        context.write(key, new Text(total + "" + " [Support: " + Double.parseDouble(dfValue.format(supportValue / total)) + ", Confidence: " + Double.parseDouble(dfValue.format(confidenceValue / total)) + "%]"));
      }
    }
  }
}