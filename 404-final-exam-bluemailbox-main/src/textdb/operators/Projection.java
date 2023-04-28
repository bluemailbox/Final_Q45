package textdb.operators;

import java.io.IOException;

import textdb.functions.Expression;
import textdb.functions.ExtractAttribute;
import textdb.relation.Relation;
import textdb.relation.Attribute;
import textdb.relation.Tuple;


/**
 * Performs relational projection (including attribute renaming and reordering).
 */
public class Projection extends Operator
{		
	/**
	 * Expressions evaluated in projection (may just be simple attributes)
	 */
	protected Expression[] expressionList;			
	
	/**
	 * Input operator
	 */
	private Operator input;
	
	public Projection(Operator in, Expression[] exprList)
	{	super(new Operator[] {in}, 0, 0);
		input = in;
		expressionList = exprList;
	
		Attribute []attr = null;

		// Build output relation assuming all expressions are simply extracting attributes
		if (expressionList != null)
		{
			attr = new Attribute[expressionList.length];
			Attribute inAttr;
			Relation inputRelation = input.getOutputRelation();
			for (int i=0; i < expressionList.length; i++)
			{
				Expression expr = expressionList[0];
				if (expr instanceof ExtractAttribute)
					inAttr = inputRelation.getAttribute( ((ExtractAttribute) expr).getAttributeLoc());
				else
					inAttr = new Attribute("Field"+i, Attribute.TYPE_STRING, 50);	// Assume string attribute if have no info
				attr[i] = inAttr;
			}
		}
		outputRelation = new Relation(attr);
		setOutputRelation(outputRelation);	
	}

	public void init() throws IOException
	{	input.init();		
	}

	public Tuple next() throws IOException
	{	Tuple inTuple = input.next();
		incrementTuplesRead();

		if (inTuple == null)
			return null;		
		
		Object[] vals = new Object[this.expressionList.length];
		for (int i=0; i < this.expressionList.length; i++)
			if(expressionList[i] != null) 
			{	
				vals[i] = expressionList[i].evaluate(inTuple);
			}
		
		incrementTuplesOutput();
		return new Tuple(vals,getOutputRelation());
	}

	public void close() throws IOException
	{	super.close();
	}
	
	public String toString()
	{	StringBuffer sb = new StringBuffer(250);
		sb.append("PROJECT: ");
		sb.append(expressionList[0].toString(input.getOutputRelation()));
		for (int i=1; i < expressionList.length; i++)
			sb.append(", "+expressionList[i].toString(input.getOutputRelation()));
		return sb.toString();
	}
}

