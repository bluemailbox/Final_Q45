package textdb.functions;

import java.text.DateFormat;
import java.text.SimpleDateFormat;

import textdb.relation.Attribute;
import textdb.relation.Tuple;

/**
 * An expression/function that stores a constant value (as an object).
 */
public class ConstantValue extends Expression
{			
	private static final long serialVersionUID = 1L;
	
	private Object value;
	
    public ConstantValue(Object obj)
    {	// Computes the type of the object and does any class casting if necessary
    	value = computeType(obj);	
    }
   
    
    public Object evaluate(Tuple t)
    {	return value;
    }
    
    public String toString()
	{	
    	if (value instanceof String)
    		return "'"+value.toString()+"'";
    	
    	if (Attribute.isNumberType(returnType))
    		return value.toString();
    	else if (Attribute.isDateType(returnType))
    	{
    		if (returnType == Attribute.TYPE_DATE)
    		{	DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
    			return "'"+df.format(value)+"'";
    		}
    		else if (returnType == Attribute.TYPE_TIME)
    		{	DateFormat df = new SimpleDateFormat("hh:mm:ss");
				return "'"+df.format(value)+"'";
    		}
    		else 
    		{	DateFormat df = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
				return "'"+df.format(value)+"'";
    		}
    	}
    	return "'"+value.toString()+"'";
	}
}