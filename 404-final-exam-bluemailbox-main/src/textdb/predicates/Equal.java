package textdb.predicates;

import java.sql.SQLException;

/**
 * A predicate for testing the equality of two objects.
 */
public class Equal extends Predicate
{	
	public boolean evaluate(Object arg1, Object arg2) throws SQLException
	{	
	    return Equal.isEqual(arg1, arg2);
	}
	
	public String toString()
	{	return "="; }
		
	@SuppressWarnings("rawtypes")
	public static boolean isEqual(Object arg1, Object arg2) throws SQLException
	{
	    if (arg1 == null || arg2 == null)
		return false;
	    
	    Class classArg1 = arg1.getClass();
	    Class classArg2 = arg2.getClass();
	    
	    if (classArg1 == classArg2)
	    {	if (arg1 instanceof String)
        	{
        		return Equal.compareStrings((String) arg1, (String) arg2);        			                    	
        	}
	    	else
	    		return arg1.equals(arg2);	// Use built in equals method for same classes
	    }
	    
	    if (arg1 instanceof java.lang.Number || arg2 instanceof java.lang.Number)
	    {
		if (arg1 instanceof java.lang.Double || arg1 instanceof java.math.BigDecimal || arg1 instanceof java.lang.Float
			|| arg2 instanceof java.lang.Double || arg2 instanceof java.math.BigDecimal || arg2 instanceof java.lang.Float)
		{ // Compare as double values
		    return ( (Number) arg1).doubleValue() == ( (Number) arg2).doubleValue();
		}
		else
		    // Comnpare as integer values
		    return ( (Number) arg1).longValue() == ( (Number) arg2).longValue();
	    }
	    else if (arg1.getClass() == arg2.getClass())
			return ((Comparable) arg1).equals(arg2);
		else
		{	// Convert objects to same class for comparison (if possible)
			Object []vals = Predicate.convertTypes(arg1, arg2);
			return ((Comparable) vals[0]).equals(vals[1]);						
		}	   	    
	}
	
	public static boolean compareStrings(String s1, String s2)
	{		
		int len1 = s1.length();
		int len2 = s2.length();
		if (len1 != len2)
		{	if (len1 > len2)
			// Pad s2
				s2 = String.format("%1$-" + len1 + "s", s2);
			else
				s1 = String.format("%1$-" + len2 + "s", s1);
		}
		return s1.equals(s2);				
	}
}

