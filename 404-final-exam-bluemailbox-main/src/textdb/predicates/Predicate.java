package textdb.predicates;

import java.sql.SQLException;

/**
 * An abstract class for representing and evaluating generic predicate expressions.
 */
public abstract class Predicate 
{
	public abstract boolean evaluate(Object arg1, Object arg2) throws SQLException;
	
	public static Object[] convertTypes(Object arg1, Object arg2) throws SQLException
	{		
		if (arg1 instanceof String || arg2 instanceof String)
		{	// Compare both as strings
			if (arg1 instanceof String)
				arg2 = arg2.toString();
			else
				arg1 = arg1.toString();
		}
		
		return new Object[]{arg1, arg2};	
	}
}