package org.anc.lapps.gigaword;

import org.junit.*;
import org.lappsgrid.api.DataSource;
import static org.lappsgrid.discriminator.Discriminators.Uri;
import org.lappsgrid.serialization.Data;
import org.lappsgrid.serialization.Serializer;
import org.lappsgrid.serialization.datasource.GetRequest;

import java.io.IOException;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * @author Keith Suderman
 */
public class GigawordSourceTests
{
	public GigawordSourceTests()
	{

	}

	@Test
	public void testGet()
	{
		String key = "NYT_ENG_20050622.0308";
		DataSource source = new GigawordSource();
		Data<String> request = new GetRequest(key);
		String json = source.execute(request.asJson());
		Data<Object> data = Serializer.parse(json, Data.class);
		String type = data.getDiscriminator();
		Object payload = data.getPayload();
		assertFalse(payload.toString(), Uri.ERROR.equals(type));
		assertTrue("Wrong discriminator used for return type.", Uri.LDC.equals(type));
		System.out.println(type);
//		json = Serializer.toPrettyJson(payload);
//		System.out.println("Size: " + json.length());
	}
}
