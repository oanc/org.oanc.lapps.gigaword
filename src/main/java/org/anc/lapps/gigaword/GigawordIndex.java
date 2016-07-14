package org.anc.lapps.gigaword;

import org.anc.index.api.Index;
import org.anc.index.core.IndexImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GigawordIndex implements Index
{
	private static Logger logger = LoggerFactory.getLogger(GigawordIndex.class);

	protected Index index;

	public GigawordIndex()
	{
		try
		{
			index = loadIndex();
		}
		catch (IOException e)
		{
			logger.error("Unable to load Gigaword index.", e);
			index = new PrivateIndex();
		}
	}

	@Override
	public int size()
	{
		return index.size();
	}

	@Override
	public File get(String id)
	{
		return index.get(id);

//		File headerFile = index.get(id);
//		if (headerFile == null)
//		{
//			return null;
//		}
//
//		File parent = headerFile.getParentFile();
//		String name = headerFile.getName().replace(".hdr", ".txt");
//		return new File(parent, name);
	}

	@Override
	public List<String> keys()
	{
		return new ArrayList<String>(index.keys());
	}

	private Index loadIndex() throws IOException
	{
		logger.info("Loading Gigaword index.");
		Index index;
		File file = new File("/gigaword/index");
		if (file.exists())
		{
			logger.debug("Found index file at {}", file.getPath());
			index = new PrivateIndex(file);
		}
		else
		{
			logger.debug("Using gigaword-en.index on classpath.");
			index = new IndexImpl("gigaword-en.index");
		}
		return index;
	}

	class PrivateIndex implements Index
	{
		private final Logger logger = LoggerFactory.getLogger(PrivateIndex.class);

		protected Map<String,String> index;

		public PrivateIndex()
		{
			index = new HashMap<>();
		}

		public PrivateIndex(File file) throws IOException
		{
			BufferedReader reader = null;
			index = new HashMap<>();
			try
			{
				reader = new BufferedReader(new FileReader(file));
				String line = reader.readLine();
				while (line != null)
				{
					String[] parts = line.split(" ");
					if (parts.length != 2)
					{
						String message = "Invalid input: " + line;
						logger.error(message);
						throw new IOException(message);
					}
					index.put(parts[0], parts[1]);
					line = reader.readLine();
				}
			}
			catch (FileNotFoundException e)
			{
				throw new IOException("File not found but file.exists() returned true.");
			}
			finally
			{
				if (reader != null)
				{
					 reader.close();
				}
			}
		}

		@Override
		public File get(String key)
		{
			String path = index.get(key);
			if (path == null)
			{
				return null;
			}
			return new File(path);
		}

		@Override
		public List<String> keys()
		{
			return new ArrayList(index.keySet());
		}

		@Override
		public int size()
		{
			return index.size();
		}
	}
}
