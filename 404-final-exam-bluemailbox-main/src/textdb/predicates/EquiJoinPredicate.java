package textdb.predicates;

import textdb.relation.Tuple;

/**
 * Used by joins to express an equi-join between tuples on one or more pairs of attributes.
 */
public class EquiJoinPredicate
{
	private int[] attr1Locs;			// Attribute indexes in relation 1
	private int[] attr2Locs;			// Attribute indexes in relation 2
	private int numAttrs;				// Number of attributes being compared
	private int keyType;				// Type of key

	static public int OBJECT_KEY = 3;
	static public int STRING_KEY = 2;
	static public int INT_KEY = 1;


	public EquiJoinPredicate(int[] idx1, int []idx2, int ktype)
	{	attr1Locs = idx1;
		attr2Locs = idx2;
		numAttrs = idx1.length;
		keyType = ktype;
	}

	public EquiJoinPredicate inversePredicate()
	{	return new EquiJoinPredicate(attr2Locs,attr1Locs,keyType);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public boolean isLessThan(Tuple t1, Tuple t2)
	{	for (int i=0; i < numAttrs; i++)
		{	if ( ((Comparable) t1.getObject(attr1Locs[i])).compareTo(t2.getObject(attr2Locs[i])) >= 0)
				return false;
		}
		return true;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public boolean isGreaterThan(Tuple t1, Tuple t2)
	{	for (int i=0; i < numAttrs; i++)
		{	if ( ((Comparable) t1.getObject(attr1Locs[i])).compareTo(t2.getObject(attr2Locs[i])) <= 0)
				return false;
		}
		return true;
	}

	public boolean isEqual(Tuple t1, Tuple t2)
	{	for (int i=0; i < numAttrs; i++)
		{	if (!t1.getObject(attr1Locs[i]).equals(t2.getObject(attr2Locs[i])))
				return false;
		}
		return true;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public int compare(Tuple t1, Tuple t2)
	{	for (int i=0; i < numAttrs; i++)
		{	int val = ((Comparable) t1.getObject(attr1Locs[i])).compareTo(t2.getObject(attr2Locs[i]));

			if (val != 0)
				return val;
		}
		return 0;
	}


	public int getNumAttr()				{ return numAttrs; }
	public int[] getRelation1Locs()		{ return attr1Locs; }
	public int[] getRelation2Locs()		{ return attr2Locs; }
	public int getKeyType()				{ return keyType; }

	public Object[] getValuesRelation1(Tuple t)
	{
		Object[] vals = new Object[numAttrs];

		for (int i=0; i < numAttrs; i++)
			vals[i] = t.getObject(attr1Locs[i]);

		return vals;
	}

	public Object[] getValuesRelation2(Tuple t)
	{
		Object[] vals = new Object[numAttrs];

		for (int i=0; i < numAttrs; i++)
			vals[i] = t.getObject(attr2Locs[i]);

		return vals;
	}
}

