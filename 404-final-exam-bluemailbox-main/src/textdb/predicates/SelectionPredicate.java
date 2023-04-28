package textdb.predicates;

import java.sql.SQLException;

import textdb.relation.Relation;
import textdb.relation.Tuple;

/**
 * An abstract class for representing selection predicates.
 */
public abstract class SelectionPredicate 
{	
	abstract public boolean evaluate(Tuple t) throws SQLException;
	
	abstract public String toString(Relation relation);
}

