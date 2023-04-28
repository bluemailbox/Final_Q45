package textdb.functions;

import textdb.relation.Relation;
import textdb.relation.Tuple;

/**
 * An expression/function class that extracts an attribute from a particular index in an input tuple.
 */
public class ExtractAttribute extends Expression {	
	private static final long serialVersionUID = 1L;
	
	/**
	 * Location of attribute to extract 
	 */
	private int attrLoc; 					
	
	public ExtractAttribute(int loc) {
		attrLoc = loc;
	}
	
	public ExtractAttribute(int loc, int retType) {
		attrLoc = loc;
		returnType = retType;
	}

	public Object evaluate(Tuple t) {
		return t.getObject(attrLoc);
	}
	
	public int getAttributeLoc()
	{	return attrLoc;
	}			
	
	public String toString(Relation relation)
	{	return relation.getAttribute(attrLoc).getName();
	}
}