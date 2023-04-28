package junit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.BufferedReader;
import java.util.ArrayList;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import textdb.functions.Expression;
import textdb.functions.ExtractAttribute;
import textdb.operators.BlockNestedLoopJoin;
import textdb.operators.Operator;
import textdb.operators.Projection;
import textdb.operators.TextFileScan;
import textdb.predicates.EquiJoinPredicate;
import textdb.question.ElimNDuplicate;
import textdb.relation.Attribute;
import textdb.relation.Relation;
import textdb.relation.Schema;
import textdb.relation.Tuple;
import textdb.util.FileManager;

/**
 * Tests in-memory hash join question
 */
public class TestElimNDuplicate {
	
	public static String DATA_DIR = "bin/data/";			// Change this if needed to indicate where the data and output directories are.
	public static String OUTPUT_DIR = "bin/output/";
	
	private static Schema schema;

	
	/**
	 * Initializes a schema with all the data files.
	 * 
	 * @throws Exception
	 * 		if an error occurs
	 */
	@BeforeAll
	public static void init() throws Exception {
		schema = new Schema();
		
		Attribute []rAttr = new Attribute[3];
		rAttr[0] = new Attribute("r_regionkey",Attribute.TYPE_INT,0);
		rAttr[1] = new Attribute("r_name",Attribute.TYPE_STRING,15);
		rAttr[2] = new Attribute("r_comment",Attribute.TYPE_STRING,60);			
		Relation region = new Relation(rAttr, "region", DATA_DIR+"region.txt");
		schema.addTable(region);
		
		Attribute []nAttr = new Attribute[4];
		nAttr[0] = new Attribute("n_nationkey",Attribute.TYPE_INT,0);
		nAttr[1] = new Attribute("n_name",Attribute.TYPE_STRING,25);
		nAttr[2] = new Attribute("n_regionkey",Attribute.TYPE_INT,0);
		nAttr[3] = new Attribute("n_comment",Attribute.TYPE_STRING,152);			
		Relation nation = new Relation(nAttr, "nation", DATA_DIR+"nation.txt");
		schema.addTable(nation);				
	}
	
	/**
	 * Tests no duplicates	
	 */
	@Test
	public void testNoDuplicates()
	{					           	
		try
		{
			Operator op = test1();
			
			int count = compareOperatorWithOutput(op, OUTPUT_DIR+"test1.txt");		
			assertEquals(25, count);
		}
		catch (Exception e)
		{	System.out.println(e);
			fail(); 
		}
	}
	
	/**
	 * Tests duplicates	with one column tuple
	 */
	@Test
	public void testDuplicatesOneColumnTuple()
	{					           	
		try
		{
			Operator op = test2();
			
			int count = compareOperatorWithOutput(op, OUTPUT_DIR+"test2.txt");		
			assertEquals(5, count);
		}
		catch (Exception e)
		{	System.out.println(e);
			fail(); 
		}
	}

	/**
	 * Tests duplicates	with one column tuple and up to 2 duplicates allowed.
	 */
	@Test
	public void testDuplicatesOneColumnTupleTwoDuplicates()
	{					           	
		try
		{
			Operator op = test3();
			
			int count = compareOperatorWithOutput(op, OUTPUT_DIR+"test3.txt");		
			assertEquals(10, count);
		}
		catch (Exception e)
		{	System.out.println(e);
			fail(); 
		}
	}


	/**
	 * Tests duplicates	with multi-column tuple
	 */
	@Test
	public void testDuplicatesMultiColumnTuple()
	{					           	
		try
		{
			Operator op = test4();
			
			int count = compareOperatorWithOutput(op, OUTPUT_DIR+"test4.txt");		
			assertEquals(5, count);
		}
		catch (Exception e)
		{	System.out.println(e);
			fail(); 
		}
	}

	/**
	 * Tests duplicates	with multi-column tuple and up to 4 duplicates allowed.
	 */
	@Test
	public void testDuplicatesMultiColumnTupleMultipleDuplicates()
	{					           	
		try
		{
			Operator op = test5();
			
			int count = compareOperatorWithOutput(op, OUTPUT_DIR+"test5.txt");		
			assertEquals(20, count);
		}
		catch (Exception e)
		{	System.out.println(e);
			fail(); 
		}
	}


	public static Operator test1() 
	{
		// Use nation table as input
		Attribute []nAttr = new Attribute[4];
		nAttr[0] = new Attribute("n_nationkey",Attribute.TYPE_INT,0);
		nAttr[1] = new Attribute("n_name",Attribute.TYPE_STRING,25);
		nAttr[2] = new Attribute("n_regionkey",Attribute.TYPE_INT,0);
		nAttr[3] = new Attribute("n_comment",Attribute.TYPE_STRING,152);			
		Relation nation = new Relation(nAttr, "nation", TestElimNDuplicate.DATA_DIR+"nation.txt");

		/* Table scan operator */
		TextFileScan nscan = new TextFileScan(TestElimNDuplicate.DATA_DIR+"nation.txt", nation);		

		/* Projection to only get n_nationkey */
		Expression[] expr = new Expression[1];
		expr[0] = new ExtractAttribute(0);  		

		Projection proj = new Projection(nscan, expr);
		ElimNDuplicate dup = new ElimNDuplicate(new Operator[]{proj}, 1);	
		
		return dup;
	}

	public static Operator test2()
	{
		// Use nation table as input
		Attribute []nAttr = new Attribute[4];
		nAttr[0] = new Attribute("n_nationkey",Attribute.TYPE_INT,0);
		nAttr[1] = new Attribute("n_name",Attribute.TYPE_STRING,25);
		nAttr[2] = new Attribute("n_regionkey",Attribute.TYPE_INT,0);
		nAttr[3] = new Attribute("n_comment",Attribute.TYPE_STRING,152);			
		Relation nation = new Relation(nAttr, "nation", TestElimNDuplicate.DATA_DIR+"nation.txt");

		/* Table scan operator */
		TextFileScan nscan = new TextFileScan(TestElimNDuplicate.DATA_DIR+"nation.txt", nation);		

		/* Projection to only get n_regionkey */
		Expression[] expr = new Expression[1];
		expr[0] = new ExtractAttribute(2);  		

		Projection proj = new Projection(nscan, expr);
		ElimNDuplicate dup = new ElimNDuplicate(new Operator[]{proj}, 1);
		
		return dup;
	}

	public static Operator test3()
	{
		// Use nation table as input
		Attribute []nAttr = new Attribute[4];
		nAttr[0] = new Attribute("n_nationkey",Attribute.TYPE_INT,0);
		nAttr[1] = new Attribute("n_name",Attribute.TYPE_STRING,25);
		nAttr[2] = new Attribute("n_regionkey",Attribute.TYPE_INT,0);
		nAttr[3] = new Attribute("n_comment",Attribute.TYPE_STRING,152);			
		Relation nation = new Relation(nAttr, "nation", TestElimNDuplicate.DATA_DIR+"nation.txt");

		/* Table scan operator */
		TextFileScan nscan = new TextFileScan(TestElimNDuplicate.DATA_DIR+"nation.txt", nation);		

		/* Projection to only get n_regionkey */
		Expression[] expr = new Expression[1];
		expr[0] = new ExtractAttribute(2);  		

		Projection proj = new Projection(nscan, expr);
		ElimNDuplicate dup = new ElimNDuplicate(new Operator[]{proj}, 2);
		
		return dup;
	}

	public static Operator test4()
	{
		// Use nation table as input
		Attribute []nAttr = new Attribute[4];
		nAttr[0] = new Attribute("n_nationkey",Attribute.TYPE_INT,0);
		nAttr[1] = new Attribute("n_name",Attribute.TYPE_STRING,25);
		nAttr[2] = new Attribute("n_regionkey",Attribute.TYPE_INT,0);
		nAttr[3] = new Attribute("n_comment",Attribute.TYPE_STRING,152);			
		Relation nation = new Relation(nAttr, "nation", TestElimNDuplicate.DATA_DIR+"nation.txt");

		Attribute []rAttr = new Attribute[3];
		rAttr[0] = new Attribute("r_regionkey",Attribute.TYPE_INT,0);
		rAttr[1] = new Attribute("r_name",Attribute.TYPE_STRING,15);
		rAttr[2] = new Attribute("r_comment",Attribute.TYPE_STRING,60);			
		Relation region = new Relation(rAttr, "region", TestElimNDuplicate.DATA_DIR+"region.txt");		
	 
		/* Table scan operator */
		TextFileScan nscan = new TextFileScan(TestElimNDuplicate.DATA_DIR+"nation.txt", nation);		
		TextFileScan rscan = new TextFileScan(TestElimNDuplicate.DATA_DIR+"region.txt", region);
		
		EquiJoinPredicate jpred = new EquiJoinPredicate(new int[]{0}, new int[]{2}, 1);
		BlockNestedLoopJoin bnlj = new BlockNestedLoopJoin(new Operator[]{rscan, nscan}, jpred, 1000, 10);											

		/* Projection to only get r_regionkey, r_name, n_regionkey */
		Expression[] expr = new Expression[3];
		expr[0] = new ExtractAttribute(0);  		
		expr[1] = new ExtractAttribute(1);  
		expr[2] = new ExtractAttribute(5);  

		Projection proj = new Projection(bnlj, expr);
		ElimNDuplicate dup = new ElimNDuplicate(new Operator[]{proj}, 1);
		
		return dup;
	}

	public static Operator test5()
	{
		// Use nation table as input
		Attribute []nAttr = new Attribute[4];
		nAttr[0] = new Attribute("n_nationkey",Attribute.TYPE_INT,0);
		nAttr[1] = new Attribute("n_name",Attribute.TYPE_STRING,25);
		nAttr[2] = new Attribute("n_regionkey",Attribute.TYPE_INT,0);
		nAttr[3] = new Attribute("n_comment",Attribute.TYPE_STRING,152);			
		Relation nation = new Relation(nAttr, "nation", TestElimNDuplicate.DATA_DIR+"nation.txt");

		Attribute []rAttr = new Attribute[3];
		rAttr[0] = new Attribute("r_regionkey",Attribute.TYPE_INT,0);
		rAttr[1] = new Attribute("r_name",Attribute.TYPE_STRING,15);
		rAttr[2] = new Attribute("r_comment",Attribute.TYPE_STRING,60);			
		Relation region = new Relation(rAttr, "region", TestElimNDuplicate.DATA_DIR+"region.txt");		
	 
		/* Table scan operator */
		TextFileScan nscan = new TextFileScan(TestElimNDuplicate.DATA_DIR+"nation.txt", nation);		
		TextFileScan rscan = new TextFileScan(TestElimNDuplicate.DATA_DIR+"region.txt", region);
		
		EquiJoinPredicate jpred = new EquiJoinPredicate(new int[]{0}, new int[]{2}, 1);
		BlockNestedLoopJoin bnlj = new BlockNestedLoopJoin(new Operator[]{rscan, nscan}, jpred, 1000, 10);											

		/* Projection to only get r_regionkey, r_name, n_regionkey */
		Expression[] expr = new Expression[3];
		expr[0] = new ExtractAttribute(0);  		
		expr[1] = new ExtractAttribute(1);  
		expr[2] = new ExtractAttribute(5);  

		Projection proj = new Projection(bnlj, expr);
		ElimNDuplicate dup = new ElimNDuplicate(new Operator[]{proj}, 4);
		
		return dup;
	}

	public static void outputOperator(Operator op) throws Exception
	{		
		Tuple t;		
		op.init();
		while ( (t = op.next()) != null)
		{
			System.out.println(t);			
		}
		op.close();
	}

	/**
	 * Compares the output of an operator with the expected output stored in a file.
	 * Returns a count of the number of records output by the operator.
	 * 
	 * @param op
	 * 		operator
	 * @param fileName
	 * 		name of file with data to compare
	 * @return
	 * 		number of records output by operator
	 */
	public static int compareOperatorWithOutput(Operator op, String fileName)
	{
		long startTime = System.currentTimeMillis();		
		String opOutput, fileOutput;	
		ArrayList<String> differences = new ArrayList<String>();
		int count = 0;	

		try
		{
			BufferedReader reader = FileManager.openTextInputFile(fileName);
			op.init();
	
			// Tuple t = new Tuple(op.getOutputRelation());
			Tuple t = new Tuple();					
			
			while ( (t = op.next()) != null)
			{
				System.out.println(t);				// Should comment this out for large files
				// t.writeText(out);
				opOutput = t.toString().trim();
				if (reader.ready())
					fileOutput = reader.readLine().trim();
				else
					fileOutput = "";
				
				if (!opOutput.equals(fileOutput))
					differences.add("Yours: "+opOutput+" Solution: "+fileOutput);
				count++;
				if (count % 10000 == 0)
					System.out.println("Total results: "+count+" in time: "+(System.currentTimeMillis()-startTime));
			}
			FileManager.closeFile(reader);
			op.close();
		}
		catch (Exception e)
		{
			System.out.println("ERROR: "+e);
			fail();
		}		
		
		long endTime = System.currentTimeMillis();
		System.out.println("Total results: "+count+" in time: "+(endTime-startTime));
		
		if (differences.size() == 0)
			System.out.println("NO DIFFERENCES!");
		else
		{
			System.out.println("DIFFERENCES: "+differences.size());
			for (int i=0; i < differences.size(); i++)
				System.out.println(differences.get(i));
			fail();
		}
		return count;
	}			
}
