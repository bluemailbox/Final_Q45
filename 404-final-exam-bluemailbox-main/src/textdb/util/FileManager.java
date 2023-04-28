package textdb.util;

import java.io.*;

import textdb.operators.Operator;
import textdb.relation.Tuple;

/**
 * A class that performs file operations such as opening and closing files (and hides the associated exceptions). 
 */
public class FileManager
{
	static public BufferedReader openTextInputFile(String fname) throws FileNotFoundException
	{	return new BufferedReader(new FileReader(fname));
	}

	static public PrintWriter openTextOutputFile(String fname) throws IOException
	{	return new PrintWriter(new BufferedWriter(new FileWriter(fname)));
	}

	static public BufferedInputStream openInputFile(String fname) throws FileNotFoundException
	{	return new BufferedInputStream(new FileInputStream(fname));
	}

	static public BufferedOutputStream openOutputFile(String fname) throws IOException
	{	return new BufferedOutputStream(new FileOutputStream(fname));
	}

	static public BufferedOutputStream appendOutputFile(String fname) throws IOException
	{	return new BufferedOutputStream(new FileOutputStream(fname, true));
	}

	static public void closeFile(BufferedReader breader) throws IOException
	{	breader.close();
	}

	static public void closeFile(PrintWriter pwriter) throws IOException
	{	pwriter.close();
	}

	static public void closeFile(BufferedInputStream f) throws IOException
	{	f.close();
	}

	static public void closeFile(BufferedOutputStream f) throws IOException
	{	f.close();
	}

	static public String getFileName(String pathName)
	{	// Returns just the fileName stripped of any previous path information in String
		int idx = pathName.lastIndexOf("/");
		if (idx == -1)
			return pathName;
		else
			return pathName.substring(idx+1);
	}

	static public String getPath(String pathName)
	{	// Returns just the path with the file name stripped off
		int idx = pathName.lastIndexOf("/");
		if (idx == -1)
			return "";
		else
			return pathName.substring(0,idx+1);
	}

	static public String createTempFileName(String hint)
	{	long time = System.currentTimeMillis();
		return hint+"_"+time+".dat";
	}

	static public void deleteFile(String fname)
	{	File f = new File(fname);
		f.delete();
	}
	
	static public void writeOutputToFile(String fname, Operator op) throws IOException
	{
		PrintWriter out = FileManager.openTextOutputFile(fname);
		op.init();

		Tuple t = new Tuple(op.getOutputRelation());
		@SuppressWarnings("unused")
		int count = 0;			
		
		while ( (t = op.next()) != null)
		{		
			t.writeText(out);
			count++;
		}
		op.close();
		out.close();
	}
}
