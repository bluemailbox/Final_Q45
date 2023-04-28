package textdb.functions;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;

import textdb.relation.Attribute;
import textdb.relation.Relation;
import textdb.relation.Tuple;

/**
 * An abstract class for representing and evaluating generic expressions.
 */
public abstract class Expression implements Serializable
{
	private static final long serialVersionUID = 1L;
	
	protected Expression parent;				// Reference to this expression's parent node
	protected ArrayList<Expression> children;	// This expression's children: can be zero to multiple		
	protected int returnType;					// Return type of expression
	
	public Expression()
	{	children = new ArrayList<Expression>(2);
		parent = null;
		returnType = -1;
	}
	
	public abstract Object evaluate(Tuple t);
	
	public int getReturnType() 					{ return returnType; }
	public void setReturnType( int iType ) 		{ returnType = iType; }

	public Expression getParent() 				{ return this.parent; }
	public void setParent(Expression parent) 	{ this.parent = parent;}	
	
	public boolean hasChildren() { return (this.getNumChildren() != 0); }
	public int getNumChildren() { return (this.children == null) ? 0 : this.children.size(); }
	public Expression getChild(int index) { return (index >= this.getNumChildren() || index < 0) ? null : (Expression)(this.children.get(index)); }

	public Object computeType(Object input)
	{	// Set type based on object class 
		String objClass = input.getClass().getName();
		if (objClass.equals("java.lang.String"))
		{	// Check to see if can cast to integer or double (maybe this could be done somewhere else)
			String val = (String) input;
			// Remove the quotes around string
			if (val.length() < 2)	// String without quotes, probably for "*"
				return val;
			val = val.substring(1,val.length()-1);			
			returnType = Attribute.TYPE_STRING;
			return val;
		}
		else if (objClass.equals("java.lang.Integer"))
		{	returnType = Attribute.TYPE_INT;
			return input;
		}
		else if (objClass.equals("java.lang.Double"))
		{	returnType = Attribute.TYPE_DOUBLE;
			return input;
		}
		else if (input instanceof Date)
		{	returnType = Attribute.TYPE_DATE;	// This may be overridden later if it is time or timestamp
			return input;
		}
		else
		{	// Everything else falls into a String
			returnType = Attribute.TYPE_STRING;
			return input.toString();
		}
	}
	
	// These two methods are defaults in case not defined in underlying classes and will allow easy invocation on superclass.
	public String toString(Relation relation)
	{	return toString();
	}
	
	public String toString(Relation relation, Attribute outputAttribute)
	{	return toString();
	}
		
	public static ArrayList<Integer> getAttributeIndexReferences(Expression exp)
	{
		ArrayList<Integer> a = new ArrayList<Integer>();
		if (exp instanceof ExtractAttribute)
			a.add(Integer.valueOf(((ExtractAttribute) exp).getAttributeLoc()));
		else
		{
			for(int i = 0 ; i < exp.getNumChildren();i++){
				Expression child = (Expression) exp.getChild(i);
				a.addAll(getAttributeIndexReferences(child));
			}
		}
		return a;		
	}
		
	public int[] getExprLocs()
	{	ArrayList<Integer> res = Expression.getAttributeIndexReferences(this);
		int[] r = new int[res.size()];
		for (int i=0; i < res.size(); i++)
			r[i] = ((Integer) res.get(i)).intValue();
		return r;
	}	
}