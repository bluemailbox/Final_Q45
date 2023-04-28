package textdb.operators;

import java.io.IOException;
import java.sql.SQLException;

import textdb.predicates.SelectionPredicate;
import textdb.relation.Tuple;

/**
 * Performs relational selection with arbitrary predicates.
 */
public class Selection extends Operator
{	
	protected SelectionPredicate predicate;
	
	private Operator input;


	public Selection(Operator in, SelectionPredicate p)
	{	super(new Operator[] {in}, 0, 0);
		input = in;
		predicate = p;
		setOutputRelation(input.getOutputRelation());			// Set output relation of this operator
	}

	public void init() throws IOException
	{
		input.init();
	}

	public Tuple next() throws IOException
	{	Tuple inTuple;

		try
		{
			while ( (inTuple = input.next()) != null)
			{	incrementTuplesRead();
				if (predicate.evaluate(inTuple))
				{	incrementTuplesOutput();
					return inTuple;
				}
			}
		}
		catch (SQLException e)
		{	throw new IOException("Predicate evaluation error: "+e); }		

		return null;											// Exhausted input relation
	}

	public void close() throws IOException
	{	super.close();
	}
	
	public String toString()
	{	StringBuffer sb = new StringBuffer(250);
		sb.append("SELECT: ");
		sb.append(predicate.toString(outputRelation));
		return sb.toString();
	}

	public SelectionPredicate getPredicate() {
		return predicate;
	}
}

