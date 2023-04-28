package textdb.predicates;

import java.sql.SQLException;

import textdb.relation.Relation;
import textdb.relation.Tuple;

/**
 * A predicate implementing logical AND.
 */
public class And extends SelectionPredicate
{	
	private SelectionPredicate predicate1;
	private SelectionPredicate predicate2;

	public And(SelectionPredicate p1, SelectionPredicate p2)
	{	predicate1 = p1;
		predicate2 = p2;
	}

	public boolean evaluate(Tuple t) throws SQLException
	{	boolean b = predicate1.evaluate(t);
		if (b)
			return predicate2.evaluate(t);
		return false;
	}
	
	public String toString(Relation relation)
	{	return predicate1.toString(relation)+" AND "+predicate2.toString(relation);
	}
}

