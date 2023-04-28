package textdb.predicates;

import java.sql.SQLException;

/**
 * A predicate for testing if arg1 > arg2.
 */
public class Greater extends Predicate
{
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public boolean evaluate(Object arg1, Object arg2) throws SQLException
	{	
		try
		{
			if (arg1 == null)
				return false;
			else if (arg2 == null)
				return false;
			else
			{
				if (arg1 instanceof java.lang.Number && arg2 instanceof java.lang.Number)
			    {
					if (arg1 instanceof java.lang.Double || arg1 instanceof java.math.BigDecimal || arg1 instanceof java.lang.Float
						|| arg2 instanceof java.lang.Double || arg2 instanceof java.math.BigDecimal || arg2 instanceof java.lang.Float)
					{ // Compare as double values
					    return ( (Number) arg1).doubleValue() > ( (Number) arg2).doubleValue();
					}
					else
					    // Comnpare as integer values
					    return ( (Number) arg1).longValue() > ( (Number) arg2).longValue();
			    }
				else if (arg1.getClass() == arg2.getClass())
					return ((Comparable) arg1).compareTo(arg2) > 0;
				else
				{	// Convert objects to same class for comparison (if possible)
					Object []vals = Predicate.convertTypes(arg1, arg2);
					return ((Comparable) vals[0]).compareTo(vals[1]) > 0;						
				}
			}
		}
		catch (Exception e)
		{	throw new SQLException("Error in > comparison.  Value1: "+arg1+" ("+arg1.getClass()+") Value2: "+arg2+"("+arg2.getClass()+") Error message: "+e); }
	}
	
	public String toString()
	{	return ">"; }
}

