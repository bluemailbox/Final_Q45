package textdb.predicates;

import java.sql.SQLException;

import textdb.relation.Relation;
import textdb.relation.Tuple;

/**
 * Selection predicates of the form: attribute op constant.
 */
public class ConstantSelectionPredicate extends SelectionPredicate
{
	private int attr1Index;					// Index of attribute 1 in tuple
	private Object constValue;				// Constant object
	private Predicate predicate;			// Simple predicate that allows evaluation of the operation

	public ConstantSelectionPredicate(int idx1, Object ct, Predicate p)
	{	attr1Index = idx1;
		constValue = ct;
		predicate = p;
	}

	public boolean evaluate(Tuple t) throws SQLException
	{	return predicate.evaluate(t.getObject(attr1Index), constValue);
	}
	
	public String toString(Relation relation)
	{	return relation.getAttribute(attr1Index).toString()+" "+predicate.toString()+" "+constValue.toString();
	}
}

