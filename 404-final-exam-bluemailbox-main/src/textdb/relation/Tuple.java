package textdb.relation;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.StringTokenizer;

import textdb.util.Convert;

/**
 * Stores a row of values.
 */
public class Tuple
{
	/**
	 * Array of field values
	 */
	protected Object[] values;						
	
	/**
	 * Relational schema describing tuple format
	 */
	protected Relation relation;						

	
	public Tuple(Relation r) 						
	{	
		relation = r; 
		values = new Object[r.getNumAttributes()]; 
	}
	
	public Tuple()
	{
		relation = new Relation();
		values = null;
	}
	
	public Tuple(Object[] vals, Relation r) 		
	{
		relation = r;
		values = new Object[vals.length];
		for (int i=0; i < vals.length; i++)
			values[i] = vals[i];
	}

	public Tuple(Tuple t)
	{	relation = t.relation;
		values = new Object[t.values.length];
		for (int i=0; i < t.values.length; i++)
			values[i] = t.values[i];
	}
	
	public Tuple(Tuple t1, Tuple t2, Relation r)
	{	relation = r;
		values = new Object[t1.values.length+t2.values.length];
		for (int i=0; i < t1.values.length; i++)
			values[i] = t1.values[i];

		for (int i=0; i < t2.values.length; i++)
			values[i+t1.values.length] = t2.values[i];
	}

	
	// Set Methods
	public Object[] getValues() 					{	return values; }
	public void setValues (Object[] vals) 			{	values = vals; }
	public int numValues()							{ 	return values.length; }
	public void setValue(int i, Object o)			{	values[i] = o; }
	public Relation getRelation()					{	return relation;}
	
	public String getString(int i) 					
	{	if (values[i] == null)
			return null;
		else
			return values[i].toString(); 
	}
	public Object getObject(int i) 					{	return values[i]; }
	
	public boolean isNull(int i)					{	return (values[i] == null); }
	
	public int getInt(int i)
	{	if (values[i] instanceof java.lang.String)
			return Integer.parseInt((String) values[i]);
		else if (values[i] instanceof java.lang.Number)
			return ((Number) values[i]).intValue();
        else
            return -1;
	}
	
	public Object[] getValuesCopy(){
		Object [] vals = new Object[values.length];
		for(int i=0; i<values.length;i++)
			vals[i] = values[i];
		return vals;
	}

	// I/O Methods
	public boolean readText(BufferedReader in) throws IOException
	{			
		String st = in.readLine();
		if (st == null)
			return false;

		StringTokenizer myTokenizer = new StringTokenizer(st, "\t");
		int numVals = myTokenizer.countTokens();

		values = new Object[numVals];

		for (int i=0; i < numVals; i++)
		{	String val = myTokenizer.nextToken();

				int attrType = relation.getAttributeType(i);

				if (attrType == Attribute.TYPE_INT)
					values[i] = Integer.valueOf(Integer.parseInt((String) val));
				else if (attrType == Attribute.TYPE_DOUBLE)
					values[i] = Double.valueOf(Double.parseDouble((String) val));
				else
					values[i] = val; 					// type string by default
		}
		return true;
	}

	public void writeText(PrintWriter out)
	{	for (int i=0; i < numValues(); i++)
			out.print(getObject(i)+"\t");
		out.println();
	}

	public boolean read(BufferedInputStream in) throws IOException
	{	// Read the tuple from disk into memory

		int header;
		byte []tmp = new byte[1000];

		header = in.read();							// Read record header
		if (header == -1)
			return false;

		in.read(tmp, 0, 4);							// Read record length

		int numFields = Convert.toInt(tmp);
		if (numFields > 10000)
			throw new IOException("I/O error.  Reading tuple from disk");
		short []offsets = new short[numFields];
		boolean [] isNull = new boolean[numFields];

		// Read the NULL bit array and offsets
		for (int i=0; i < numFields; i++)
			isNull[i] = (in.read() > 0);

		for (int i=0; i < numFields; i++)
		{	if (!isNull[i])
			{
				in.read(tmp, 0, 2);				// Read short offset
				offsets[i] = Convert.toShort(tmp);
			}
		}

		// Read the data
		for (int i=0; i < numFields; i++)
			if (!isNull[i])
				values[i] = Attribute.read(in, relation.getAttributeType(i));
			else
				values[i] = null;

		return true;
	}


	public void write(BufferedOutputStream out) throws IOException
	{	byte b = 1;
		short []offsets = new short[values.length];
		boolean [] isNull = new boolean[values.length];
		int curPos = 0;

		// Determine field offsets
		for (int i=0; i < values.length; i++)
		{	if (values[i] == null)
				isNull[i] = true;
			else
			{	isNull[i] = false;
				offsets[i] = (short) curPos;

				// Put into correct number of bytes - doing only int and string for now
				int attrType = relation.getAttributeType(i);
				int size = Attribute.getByteSize(attrType, values[i]);
				curPos = curPos +  size;
			}
		}

		// Write out the record
		out.write(b);									// Record header - not currently used
		out.write(Convert.toByte(values.length));		// Number of fields
		for (int i=0; i < values.length; i++)			// Write isNULL bit array
			if (isNull[i])
				out.write(1);
			else
				out.write(0);

		for (int i=0; i < values.length; i++)			// Write field offsets
			if (!isNull[i])
				out.write(Convert.toByte(offsets[i]));

		for (int i=0; i < values.length; i++)
			if (!isNull[i])
				Attribute.write(out, relation.getAttributeType(i), values[i]);
	}

	public String toString()
	{	StringBuffer buf = new StringBuffer();
		if (numValues() > 0)
			buf.append(getObject(0));
		for (int i=1; i < numValues(); i++)
			buf.append(","+getObject(i));
		return buf.toString();
	}
}
