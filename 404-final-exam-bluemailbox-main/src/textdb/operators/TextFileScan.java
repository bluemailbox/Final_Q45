package textdb.operators;

import java.io.*;

import textdb.relation.Relation;
import textdb.relation.Tuple;
import textdb.util.FileManager;

/**
 * Performs a file scan in iterator form.  File is assumed to be on local hard drive in TEXT form.  
 */
public class TextFileScan extends Operator
{
	protected String inFileName;					// Name of input file to scan
	protected BufferedReader inFile;				// Reader for input file
	protected Relation inputRelation;				// Schema of file being scanned
	protected String header;						// Header (first line of input file)

	public TextFileScan(String inName, Relation r)
	{	super();
		inFileName = inName;
		inputRelation = r;
		setOutputRelation(r);						// Set output relation of this operator
	}

	public void init() throws FileNotFoundException, IOException
	{	inFile = FileManager.openTextInputFile(inFileName);
		// Read first line which is header information - discarding for now
		header = inFile.readLine();		
	}

	public Tuple next() throws IOException
	{	Tuple t = new Tuple(inputRelation);

		if (!t.readText(inFile))						// Read a tuple from input
			return null;

		incrementTuplesRead();
		incrementTuplesOutput();
		return t;
	}	

	public void close() throws IOException
	{	
		if (inFile != null)
			FileManager.closeFile(inFile);
	}
}

