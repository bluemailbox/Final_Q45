package textdb.relation;

/**
 * Represents a relational schema.
 */
public class Relation
{
	private Attribute[] attributes;
	private String fileName;
	private String name;
	
	public Relation()								
	{ attributes = null; 
	}
	
	public Relation(Attribute []attrs)				
	{ attributes = attrs; 
	}
	
	public Relation(Attribute []attrs, String name, String fileName)
	{ 	attributes = attrs; 
		this.name = name;
		this.fileName = fileName;
	}

	public Relation(Relation r)
	{
		attributes = new Attribute[r.attributes.length];
		for (int i=0; i < attributes.length; i++)
			attributes[i] = new Attribute(r.attributes[i]);
	}


	public int getAttributeType(int index)			{ return attributes[index].getType(); }
	public int getNumAttributes()					{ return attributes.length; }
	public Attribute getAttribute(int index)		{ return attributes[index]; }

	public Attribute findAttributeByName(String attrName)
	{
		for (int i=0; i < attributes.length; i++)
		{	if (attributes[i].getName().equalsIgnoreCase(attrName))
				return attributes[i];
		}
		return null;
	}
	
	public int getAttributeIndex(Attribute a)
	{	
		for (int i = 0; i < attributes.length; i++)
			if (attributes[i] == a)
				return i;
		return -1;
	}
	
	public void mergeRelation(Relation r)
	{	if (attributes == null)
		{	attributes = new Attribute[r.attributes.length];

			for (int i = 0; i < r.attributes.length; i++)
				attributes[i] = r.attributes[i];
		}
		else
		{
			int len = attributes.length+r.attributes.length;
			Attribute [] attr = new Attribute[len];

			for (int i = 0; i < attributes.length; i++)
				attr[i] = attributes[i];

			for (int i= attributes.length; i < len; i++)
				attr[i] = r.attributes[i-attributes.length];

			attributes = attr;
		}
	}

	public String toString()
	{	StringBuffer sb = new StringBuffer();
		for (int i=0; i < attributes.length; i++)
		{	sb.append(attributes[i].toString());
			sb.append(" ");
		}
		return sb.toString();
	}
	
	public void setFileName(String fileName) {
		this.fileName = fileName;
	}
	
	public String getFileName() {
		return fileName;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getName() {
		return name;
	}
	
	/**
	 * Returns an attribute if found in table otherwise null.
	 * @param name
	 * @return
	 */
	public Attribute getTable(String name)
	{
		for (int i=0; i < attributes.length; i++)
		{
			Attribute a = attributes[i];
			if (a.getName().equalsIgnoreCase(name))
				return a;
		}
		return null;
	}	
}
