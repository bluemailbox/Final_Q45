package textdb.relation;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.Types;

import textdb.util.Convert;

/**
 * Represents a relational attribute.
 */
public class Attribute implements Serializable
{	
	private static final long serialVersionUID = 1L;
	
	/**
	 * Name of attribute
	 */
	private String name;		
	
	/**
	 * Type of attribute
	 */
	private int type;			
	
	/**
	 * Length of attribute
	 */
	private int length;			
	
	/**
	 * Stores a reference to any object. Intended to store GQFieldRef object reference (note may be null if this is a computed attribute).
	 */
	private Object reference;	

	// Static constants for types
	public static int TYPE_TINYINT = Types.TINYINT;
	public static int TYPE_SMALLINT = Types.SMALLINT;
	public static int TYPE_INT = Types.INTEGER;
	public static int TYPE_FLOAT = Types.FLOAT;
	public static int TYPE_DECIMAL = Types.DECIMAL;
	public static int TYPE_DOUBLE = Types.DOUBLE;
	public static int TYPE_CHAR = Types.CHAR;
	public static int TYPE_STRING = Types.VARCHAR;
	public static int TYPE_BLOB = Types.BLOB;
	public static int TYPE_DATE = Types.DATE;
	public static int TYPE_TIMESTAMP = Types.TIMESTAMP;
	public static int TYPE_TIME = Types.TIME;
	public static int TYPE_NUMBER = 99999;				// Non-SQL type.  Means maps to Java number class (accepts smallint, integer, float, double)
	public static int TYPE_SOURCEREF = 100000;
	public static int TYPE_INTERVAL = 99998;
	
	public Attribute()								{ name = ""; type = 0; }
	public Attribute(Attribute a)					{ name = a.name; type = a. type; length = a.length; reference = a.reference;}
	public Attribute(String n, int t, int l)		{ name = n; type = t; length = l; }
	public Attribute(String n, int t, int l, Object ref)	{ name = n; type = t; length = l; reference = ref;}
	
	// Get/Set Methods
	public String getName()							{ return name; }
	public void setName(String st)					{ name = st; }
	public int getType()							{ return type; }
	public void setType(int t)						{ type = t; }
	public int getLength()							{ return length; }
	public void setLength(int l)					{ length = l; }
	public void setReference(Object ref)			{ reference = ref; }
	public Object getReference()					{ return reference; }
	
	// I/O Methods
	public static Object read(BufferedInputStream in, int attrType) throws IOException
	{	byte []tmp = new byte[20];

		if (attrType == Attribute.TYPE_INT)
		{	in.read(tmp,0,4);
			return Integer.valueOf(Convert.toInt(tmp));
		}
		else if (attrType == Attribute.TYPE_SMALLINT)
		{	in.read(tmp,0,4);
			return Short.valueOf(Convert.toShort(tmp));
		}
		else if (attrType == Attribute.TYPE_DECIMAL)
		{	byte len = (byte) in.read();			
			in.read(tmp,0,len);
			tmp[len] = 0;
			return Convert.toBigDecimal(tmp,len);
		}
		else if (attrType == Attribute.TYPE_STRING || attrType == Attribute.TYPE_CHAR)
		{	in.read(tmp,0,2);
			short len = Convert.toShort(tmp);
			if (len >= 20)
				tmp = new byte[len+1];
			in.read(tmp,0,len);
			tmp[len] = 0;
			return Convert.toString(tmp,len);
		}
		else if (attrType == Attribute.TYPE_DATE)
		{	
			byte len = (byte) in.read();
			in.read(tmp,0,len);
			tmp[len] = 0;	
			return java.sql.Date.valueOf(Convert.toString(tmp,len));
		}
		else if (attrType == Attribute.TYPE_TIMESTAMP)
		{	// Type is java.sql.Timestamp
			byte len = (byte) in.read();
			in.read(tmp,0,len);
			tmp[len] = 0;
			return java.sql.Timestamp.valueOf(Convert.toString(tmp,len));
		}
		else
		{	// Treat it as a string
			in.read(tmp,0,2);
			short len = Convert.toShort(tmp);
			if (len >= 20)
				tmp = new byte[len+1];
			in.read(tmp,0,len);
			tmp[len] = 0;
			return Convert.toString(tmp,len);
		}
	}
	

	public static void write(BufferedOutputStream out, int attrType, Object obj) throws IOException
	{
		byte [] raw = null, raw2=null ;

		if (attrType == Attribute.TYPE_INT)
		{	raw = Convert.toByte( ((Integer) obj).intValue());
		}
		else if (attrType == Attribute.TYPE_SMALLINT)
		{	raw = Convert.toByte( ((Short) obj).intValue());
		}
		else if (attrType == Attribute.TYPE_DECIMAL)
		{	raw = Convert.toByte( ((BigDecimal) obj).toString());
			out.write(raw.length);						// Write out size of decimal string
		}
		else if (attrType == Attribute.TYPE_STRING || attrType == Attribute.TYPE_CHAR)
		{	raw = Convert.toByte( (String) obj);
			short len = (short) raw.length;
			raw2 = Convert.toByte(len);
			out.write(raw2,0,raw2.length);
		}
		else if (attrType == Attribute.TYPE_DATE)
		{	
			raw = Convert.toByte(obj.toString());
			out.write(raw.length);	
		}
		else if (attrType == Attribute.TYPE_TIMESTAMP)
		{	raw = Convert.toByte(obj.toString());
			out.write(raw.length);
		}
		else
		{	// Convert to string
			raw = Convert.toByte(obj.toString());
			short len = (short) raw.length;
			raw2 = Convert.toByte(len);
			out.write(raw2,0,raw2.length);
		}
		
		if (raw == null)
		{	System.out.println("Type: "+attrType+" Data: "+obj);
		}
		out.write(raw,0,raw.length);
	}
	
	// Other Methods
	public static int getByteSize(int attrType, Object obj)
	{	if (attrType == Attribute.TYPE_INT)
			return 4;
		else if (attrType == Attribute.TYPE_SMALLINT)
			return 2;
		else if (attrType == Attribute.TYPE_DECIMAL)	// Treat as a character string
			return ( ((BigDecimal) obj).toString()).length()+1;
		else if (attrType == Attribute.TYPE_STRING)
		{	if (obj instanceof String)
				return ((String) obj).length()+1;
			else if (obj instanceof Integer)
				return 4;
			else
			{	// Default to string
				String st = obj.toString();
				return st.length()+1;
			}
		}
		else	// Default to a string representation
		{	String st = obj.toString();
			return st.length()+1;
		}
	}

	public String toString()
	{	return name+":"+type+"("+length+")" + reference;
	}
	
	public String outputString()
	{	return name+":"+type+"("+length+")" + reference;
	}
    
	public static boolean isStringType(int type)
	{	if (type == TYPE_STRING || type == TYPE_CHAR)
			return true;
		return false;
	}
	
	public static boolean isDoubleType(int type)
	{	if (type == TYPE_DOUBLE || type == TYPE_FLOAT)
			return true;
		return false;
	}
	
	public static boolean isNumberType(int type)
	{	if  (type == TYPE_INT || type == TYPE_DOUBLE || type == TYPE_FLOAT || type == TYPE_NUMBER || type == TYPE_TINYINT || type == TYPE_SMALLINT || type == TYPE_DECIMAL)
			return true;
		return false;
	}
	
	public static boolean isDateType(int type)
	{	if (type == TYPE_DATE || type == TYPE_TIME || type == TYPE_TIMESTAMP)
			return true;
		return false;
	}
}
