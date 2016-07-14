package org.anc.lapps.gigaword;

import org.anc.index.api.Index;
import org.junit.*;

import java.io.File;
import java.io.IOException;
import java.util.List;

import static org.junit.Assert.*;

/**
 * @author Keith Suderman
 */
public class GigawordIndexTest
{
	public static final int INDEX_SIZE = 1216997;
	public GigawordIndexTest()
	{

	}

	@Test
	public void testIndexSize() throws IOException
	{
		Index index = new GigawordIndex();
		assertEquals(index.size(), INDEX_SIZE);
	}

	@Test
	public void testListSize() throws IOException
	{
		Index index = new GigawordIndex();
		List<String> list = index.keys();
		assertEquals(INDEX_SIZE, list.size());
		assertEquals(index.size(), list.size());
	}

	@Test
	public void allFilesExist() throws IOException
	{
		Index index = new GigawordIndex();
		List<String> keys = index.keys();
		for (String key : keys)
		{
			File file = index.get(key);
			assertTrue("Not found: " + file.getPath(), file.exists());
//			System.out.println("Found " + file.getPath());
		}
	}
}
