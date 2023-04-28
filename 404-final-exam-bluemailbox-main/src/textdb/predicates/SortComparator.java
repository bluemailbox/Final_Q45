package textdb.predicates;

import java.util.Comparator;

import textdb.relation.Tuple;

/**
 * Used by sorting algorithms to perform tuple comparisons.
 */
@SuppressWarnings("rawtypes")
public class SortComparator implements Comparator
{
	private int[] attrLocs;				// Indexes of attributes to be sorted
	private boolean[] sortAsc;			// True if attribute is to be sorted in ascending order
	private int numAttrs;				// Number of attributes being compared

	public SortComparator(int[] idx, boolean []sa)
	{	attrLocs = idx;
		sortAsc = sa;
		numAttrs = idx.length;
	}

	public void shiftComparator(int i)
	{
		for (int j = 0; j < numAttrs; j++)
			attrLocs[j] += i;
	}

	@SuppressWarnings("unchecked")
	public int compare(Object o1, Object o2)
	{	Tuple t1 = (Tuple) o1;
		Tuple t2 = (Tuple) o2;

		for (int i=0; i < numAttrs; i++)
		{	// val is positive if t1.attr > t2.attr, 0 if equal, negative otherwise
			int val = ((Comparable) t1.getObject(attrLocs[i])).compareTo(t2.getObject(attrLocs[i]));

			if (sortAsc[i])
			{	if (val > 0)
					return 1;		// T1 is greater than T2 if sorting in ascending order
				else if (val < 0)
					return -1;		// T1 is less than T2 if sorting in ascending order
			}
			else
			{	if (val < 0)
					return 1;		// T1 is greater than T2 if sorting in descending order
				else if (val > 0)
					return -1;		// T1 is less than T2 if sorting in descending order
			}
		}
		return 0;					// T1 and T2 are equal in all attributes
	}
}

