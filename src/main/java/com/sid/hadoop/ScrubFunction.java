package com.sid.hadoop;


import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class ScrubFunction extends BaseOperation implements Function
{
  public ScrubFunction(Fields fieldDeclaration)
  {
    super(fieldDeclaration);
  }

  public ScrubFunction(int numArgs, Fields fieldDeclaration)
  {
    super(numArgs, fieldDeclaration);
  }

  public ScrubFunction(int numArgs)
  {
    super(numArgs);
  }

  public void operate(FlowProcess flowProcess, FunctionCall functionCall)
  {
    Tuple result = new Tuple();
    TupleEntry argument = functionCall.getArguments();
    //System.out.println(argument.toString());
    for (int j = 0; j < argument.getFields().size(); j++) {
    String s = argument.getString( j);
    s = (s == null || s.isEmpty() ||  s.toLowerCase().contains("null")  ) ? new String("") : (s.contains("&") ? s.replace("&", "|||"):argument.getString( j ));
    //(s.contains("&") ? s.replace("&", ""): 
    /*if (s == null || s.isEmpty() ||  s.toLowerCase().contains("null") )
    {
    	s = new String("");
    }
    else
    {
    	s = argument.getString( j );
    }*/
    result.add(s);
    }
    //System.out.println(result);
    functionCall.getOutputCollector().add(result);
  }
}
