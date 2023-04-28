package textdb.operators;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;

import textdb.predicates.SortComparator;
import textdb.relation.Tuple;
import textdb.util.FileManager;

/**
 * An iterator performing a two-pass external merge sort.
 */
public class MergeSort extends Operator
{
	private Tuple[] buffer;							// Dynamic buffer of tuples (set when MergeSort is initialized)
	private int arraySize;							// Size of buffer array in tuples
	private BufferedOutputStream outFile;			// Writer for output file
	private BufferedInputStream[] mergeFile;		// Array to store readers for input files created after partition step
	private ArrayList<String> mergeFileName;		// Stores file names of partition files
	private int numFiles;							// Number of partition files
	private boolean onePass;						// True if only need one pass of input (input file fit into buffer)
	private int curTuple;							// Current tuple position in buffer (used only for one pass)
	private SortComparator sorter;					// Contains information on which attributes and how to sort them (ASC or DESC)
	private Operator input;							// Input operator (used as a convenience as can also use in[0] in Operator superclass


	public MergeSort(Operator in, int bsize, int bfr, SortComparator sc)
	{	super(new Operator[] {in}, bfr, bsize);
		mergeFileName = new ArrayList<String>(20);
		input = in;
		sorter = sc;
		arraySize = bsize*bfr;
		setOutputRelation(in.getOutputRelation());
	}

	public void init() throws IOException, FileNotFoundException
	{	input.init();

		// Initialize buffer
		buffer = new Tuple[arraySize];

		// Create sorted sublists
		partition();

		if (!onePass)
		{	// Create file array
			mergeFile = new BufferedInputStream[numFiles];

			// Open all input files and set initial cursor
			for (int i = 0; i < numFiles; ++i)
			{	mergeFile[i] = FileManager.openInputFile( (String) mergeFileName.get(i));

				buffer[i] = new Tuple(input.getOutputRelation());
				if (!buffer[i].read(mergeFile[i]))	// Read a tuple from input
					break;
			}
		}
		else
		{	// Just output tuples currently in sorted buffer
			curTuple = 0;
		}
	}

	public Tuple next() throws IOException, FileNotFoundException
	{	if (onePass)
		{	// Reading from a single file
			if (curTuple < arraySize)
				return buffer[curTuple++];
			else
				return null;
		}
		else
		{	// Merging tuples
			while (numFiles > 0)
			{
				// Find next output line
				int minIdx = 0;
				for (int i = 1; i < numFiles; i++)
				{	 if (sorter.compare(buffer[i], buffer[minIdx]) < 0)
						minIdx = i;
				}

				// Return smallest record
				Tuple t = new Tuple(buffer[minIdx]);

				 // Get the next line from this file
				if (!buffer[minIdx].read(mergeFile[minIdx]))
				{	// This file is finished - close it and replace it and delete it
					FileManager.closeFile(mergeFile[minIdx]);
					numFiles--;
					File tmpFile = new File( (String) mergeFileName.get(minIdx));
					tmpFile.delete();
					buffer[minIdx]=buffer[numFiles];
					mergeFile[minIdx]=mergeFile[numFiles];
					mergeFileName.set(minIdx,mergeFileName.get(numFiles));
					mergeFileName.remove(numFiles);
				}
				return t;
			}
			return null;	// No more tuples to return
		}
	}

	public void close() throws IOException
	{	// Input iterator is closed after partition phase
	}



	@SuppressWarnings("unchecked")
	private void partition() throws IOException, FileNotFoundException
	{
		// Create sorted sublists (partitions)
		numFiles = 0;
		while (true)
		{	int count = 0;
			while (count < arraySize)
			{	if ( (buffer[count] = input.next()) == null)						// Read a tuple from input
						break;
				count++;
			}

			// Check for single pass case
			if (numFiles==0 && (count < arraySize))	// Filled up buffer but no input left to read
			{	incrementPagesRead((int)Math.ceil((double)count/BLOCKING_FACTOR));	// As require only single read of input
				incrementTuplesRead(count);
				incrementTuplesOutput(count);
				onePass = true;
				singlePass(count);
				return;
			}

			// Sort tuples in buffer
			if (count > 0)
			{	Arrays.sort(buffer, 0, count, sorter);

				// Write out sorted sublist
				outFile = FileManager.openOutputFile(generateTmpFileName(numFiles));
				for (int i=0; i < count; i++)
					buffer[i].write(outFile);

				FileManager.closeFile(outFile);
				int pages = (int) Math.ceil((double)count/BLOCKING_FACTOR);
				incrementPagesRead(pages);		// Read from input
				incrementTuplesRead(count);
				incrementTuplesOutput(count);
				incrementTupleIOs(count*2);		// Tuple I/Os not including input
				incrementPageIOs(pages*2);		// Page I/Os not including input
				numFiles++;
			}
			else
				break;											// Read entire file
		}
		input.close();
	}

	private String generateTmpFileName(int i)
	{	String st = FileManager.createTempFileName("merge_run"+i);
		mergeFileName.add(st);
		return st;
	}

	@SuppressWarnings("unchecked")
	private void singlePass(int count) throws IOException, FileNotFoundException
	{	// Sort tuples in buffer
		Arrays.sort(buffer, 0, count, sorter);
		input.close();
	}
}