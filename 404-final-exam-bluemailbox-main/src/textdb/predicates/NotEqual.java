package textdb.predicates;

import java.sql.SQLException;

/**
 * A predicate for testing if arg1 != arg2.
 */
public class NotEqual extends Predicate
{	
	public boolean evaluate(Object arg1, Object arg2) throws SQLException
	{	return !Equal.isEqual(arg1, arg2);		     
	}
	
	public String toString()
	{	return "!="; }
}

