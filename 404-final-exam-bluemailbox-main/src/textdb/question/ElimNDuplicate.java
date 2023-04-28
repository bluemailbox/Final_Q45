package textdb.question;

import java.io.IOException;
import java.util.HashMap;

import junit.TestElimNDuplicate;
import textdb.operators.Operator;
import textdb.relation.Relation;
import textdb.relation.Tuple;


/**
 * Implements a version of one-pass duplicate elimination.
 * The operator will allow up to and including N duplicates (specified by user) before eliminating duplicates.
 * 
 * Notes:
 * 1) All fields are used to determine if tuples are duplicates. 
 * 2) You can use String version of a tuple to detect duplicates: tuple.toString().
 * 3) HINT: A HashMap may be useful to make searches faster, but that is not required. 
 */
public class ElimNDuplicate extends Operator
{
	// Iterator state variables	

	public int maxDuplicatesAllowed;

	public ElimNDuplicate(Operator []in, int maxDuplicatesAllowed)
	{	super(in, 10, 10000);	
		this.maxDuplicatesAllowed = maxDuplicatesAllowed;
	}

	public void init() throws IOException
	{
		// TODO: Initialize input
		input[0].init();
		input[1].init();

		// Create output relation to be the same as input relation
		Relation out = new Relation(input[0].getOutputRelation());		
		setOutputRelation(out);
		
		// TODO: Setup some data structure to remember tuples have seen before	
		Tuple leftTuple;
		while((leftTuple = input[0].next()) != null)
		out.add(leftTuple);
		out.getInt(buildTupleIdx);
		input[0].close();
	}


	public Tuple next() throws IOException
	{
		// TODO: Read next input tuple. Check if have seen a tuple like this before.
		Tuple rightTuple;
		while((rightTuple = input[1].next()) != null) {
			int index = righTuple.getInt(buildTupleIdx);
			if(index < out.size() && index >= 0)
			return outputJoinTuple(out.get(index), rightTuple);
		}
		return null;
	}

	public void close() throws IOException
	{	super.close();
	}

	/*
	NOTE: Main method is only if you want to test separate from JUnit test. It is JUnit tests you must run.
	*/
	public static void main(String []argv) throws Exception
	{
		Operator op = null;
		System.out.println("Performing test#1 (no duplicates): ");
		op = TestElimNDuplicate.test1();
		TestElimNDuplicate.outputOperator(op);

		System.out.println("Performing test#2 (duplicates - one attribute tuple): ");
		op = TestElimNDuplicate.test2();
		TestElimNDuplicate.outputOperator(op);

		System.out.println("Performing test#3 (two duplicates - one attribute tuple): ");
		op = TestElimNDuplicate.test3();
		TestElimNDuplicate.outputOperator(op);

		System.out.println("Performing test#4 (duplicates - multi-attribute tuple): ");
		op = TestElimNDuplicate.test4();
		TestElimNDuplicate.outputOperator(op);

		System.out.println("Performing test#5 (four duplicates - multi-attribute tuple): ");
		op = TestElimNDuplicate.test5();
		TestElimNDuplicate.outputOperator(op);
	}	
}

