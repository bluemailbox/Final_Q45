package textdb.relation;

import java.util.ArrayList;

/**
 * A schema for a database which contains all relation and attribute information.
 */
public class Schema 
{
	private ArrayList<Relation> tables;
	
	public Schema()
	{	tables = new ArrayList<Relation>();	
	}
	
	public void addTable(Relation r)
	{	tables.add(r); }
	
	/**
	 * Returns a table if found in schema otherwise null.
	 * 
	 * @param name
	 * 		table name
	 * @return
	 * 		relation
	 */
	public Relation getTable(String name)
	{
		for (int i=0; i < tables.size(); i++)
		{
			Relation r = tables.get(i);
			if (r.getName().equalsIgnoreCase(name))
				return r;
		}
		return null;
	}		
}
