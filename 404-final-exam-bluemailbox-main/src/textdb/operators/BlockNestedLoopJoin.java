package textdb.operators;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;

import textdb.predicates.EquiJoinPredicate;
import textdb.predicates.SortComparator;
import textdb.relation.Relation;
import textdb.relation.Tuple;

/**
 * Implements block nested loop join.
 * Uses a given buffer size in tuples to improve performance by reading a large chunk (block) of the smaller relation at a time.
 * 
 * Notes:
 * 1) It is assumed that the left input is the outer input and the right input is the inner input.  Thus, the left input should be the smaller input.
 * 2) Uses sorted array and binary search to quickly find tuples based on keys.
 */
public class BlockNestedLoopJoin extends Operator
{
	// Iterator state variables
	private Tuple tupleLeft;				// Current tuple of left(smaller) input
	private Tuple tupleRight;				// Current tuple of right input
	private EquiJoinPredicate pred;			// A equi-join comparison class that can handle 1 or more attributes
	private Tuple[] buffer;					// Buffer for querying tuples
	private int countBuffer;				// # of tuples in buffer
	private SortComparator sorter;			// Only necessary because data structure for buffer is sorted array
	private Tuple tmpLeft;					// Temporary tuple for probing with binary search
	private int locBuffer;					// Current index in buffer
	private boolean complete;				// Flag indicating that join is complete

	public BlockNestedLoopJoin(Operator []in, EquiJoinPredicate p, int bsize, int bfr)
	{	super(in, bfr, bsize);
		pred = p;

		// Create output relation - keep all attributes of both tuples
		Relation out = new Relation(input[0].getOutputRelation());
		out.mergeRelation(input[1].getOutputRelation());
		setOutputRelation(out);
	}

	public void init() throws IOException
	{
		// Initialize inputs
		input[0].init();
		input[1].init();

		complete = false;
		
		// Create sorter for left input
		sorter = new SortComparator(pred.getRelation1Locs(), new boolean[pred.getNumAttr()]);

		// Read first chunk of BUFFER_SIZE-1 tuples into HashMap buffer		
		buffer = new Tuple[BUFFER_SIZE-1];
		tmpLeft = new Tuple(input[0].getOutputRelation());
		readBlockLeft();
		tupleRight = input[1].next();
		if (tupleRight == null)					// Forces join to end immediately after next() is called
			countBuffer = -1;
		else
			searchStartLoc(tupleRight);
	}


	public Tuple next() throws IOException
	{
		if (complete)
			return null;

		while (true)
		{
			
			if (locBuffer >= 0 && locBuffer < countBuffer)
			{
				boolean isEqual = false;
				try
				{
					isEqual = pred.isEqual(buffer[locBuffer],tupleRight);
				}
				catch (ClassCastException ex)
				{
					String exst = "ClassCastException when comparing tuples. Usually means that you are comparing wrong field indexes." 
							+"\nFields compared were: Left: "+pred.getRelation1Locs()[0]+" ("+input[0].getOutputRelation().getAttribute(pred.getRelation1Locs()[0]).getName()+")"
							+" Right: "+pred.getRelation2Locs()[0]+" ("+input[1].getOutputRelation().getAttribute(pred.getRelation2Locs()[0]).getName()+")";
					System.out.println(exst);
					throw new IOException(exst); 
				}
				if (isEqual)
					return outputJoinTuple(buffer[locBuffer++], tupleRight);
				locBuffer++;
			}
			else
			{
				tupleRight = input[1].next();

				if (tupleRight == null)
				{	readBlockLeft();
					if (countBuffer == 0)
					{	input[1].close();
						input[0].close();
						complete = true;
						return null;				// Join is complete
					}
					else
					{
						input[1].close();
						input[1].init();
						tupleRight = input[1].next();
					}
				}
				searchStartLoc(tupleRight);
			}
		}
	}

	public void close() throws IOException
	{	super.close();
	}

	private Tuple outputJoinTuple(Tuple left, Tuple right)
	{	Tuple t = new Tuple(left, right, getOutputRelation());
		incrementTuplesOutput();
		return t;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private void readBlockLeft() throws IOException
	{
		int i;

		for (i=0; i < BUFFER_SIZE-1; i++)
		{	tupleLeft = input[0].next();
			if (tupleLeft == null)
				break;
			buffer[i] = tupleLeft;
		}
		countBuffer=i;
		Arrays.sort(buffer, 0, countBuffer, (Comparator) sorter);
		locBuffer = 0;
	}

	@SuppressWarnings("unchecked")
	private void searchStartLoc(Tuple tRight) throws IOException
	{
		try {		
			Object[] obj = pred.getValuesRelation2(tRight);
			int[] locs1 = pred.getRelation1Locs();
			for (int i=0; i < pred.getNumAttr(); i++)
				tmpLeft.setValue(locs1[i],obj[i]);

			// This is awful - should write own binary search routine!!
			Tuple[] tmpBuffer = new Tuple[countBuffer];
			for (int i=0; i < countBuffer; i++)
				tmpBuffer[i] = buffer[i];
			
			locBuffer = Arrays.binarySearch(tmpBuffer,tmpLeft,sorter);

			if (locBuffer > 0)
			{
				// Now back up because may be multiple and got an earlier one
				while (locBuffer > 0 && pred.isEqual(tmpBuffer[locBuffer-1], tRight))
					locBuffer--;
			}
		}			
		catch (ClassCastException ex)
		{
			String exst = "ClassCastException when comparing tuples. Usually means that you are comparing wrong field indexes." 
						+"\nFields compared were: Left: "+pred.getRelation1Locs()[0]+" ("+input[0].getOutputRelation().getAttribute(pred.getRelation1Locs()[0]).getName()+")"
						+" Right: "+pred.getRelation2Locs()[0]+" ("+input[1].getOutputRelation().getAttribute(pred.getRelation2Locs()[0]).getName()+")";
			System.out.println(exst);
			throw new IOException(exst); 
		}
	}
}

