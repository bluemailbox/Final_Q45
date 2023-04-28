package textdb.predicates;

import java.sql.SQLException;

import textdb.functions.Expression;
import textdb.relation.Relation;
import textdb.relation.Tuple;

/**
 * Selection predicates of the form: attribute op attribute.
 */
public class SelectionExprPredicate extends SelectionPredicate
{	
	public Expression Expr1;				// Index of Expression 1
	public Expression Expr2;				// Index of Expression 2
	private Predicate predicate;			// Simple predicate that allows evaluation of the operation


	public SelectionExprPredicate(Expression ex1, Expression ex2, Predicate p)
	{	
		Expr1 = ex1;
		Expr2 = ex2;
		predicate = p;
	}

	public boolean evaluate(Tuple t) throws SQLException
	{	return predicate.evaluate(Expr1.evaluate(t), Expr2.evaluate(t));}
	
	public String toString(Relation relation)
	{	return Expr1.toString(relation)+" "+predicate.toString()+" "+Expr2.toString(relation);
	}
	
	public Expression getLeftExpr()
	{	return Expr1; }
	
	public Expression getRightExpr()
	{	return Expr2; }
	
	public Predicate getPredicate()
	{	return predicate; }
	
	public void setLeftExpr(Expression leftExpr)
	{	Expr1 = leftExpr; }
	
	public void setRightExpr(Expression rightExpr)
	{	Expr2 = rightExpr; }
	
	public void setPredicate(Predicate pred)
	{	predicate = pred; }	
}

