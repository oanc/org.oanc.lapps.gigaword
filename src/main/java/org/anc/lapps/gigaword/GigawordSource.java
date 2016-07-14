package org.anc.lapps.gigaword;

import org.lappsgrid.discriminator.Discriminators;
import org.lappsgrid.metadata.DataSourceMetadata;
import org.lappsgrid.serialization.Data;
import org.lappsgrid.serialization.DataContainer;
import org.lappsgrid.serialization.lif.Container;
import static org.lappsgrid.discriminator.Discriminators.Uri;

import java.io.IOException;

/**
 * @author Keith Suderman
 */
public class GigawordSource extends AbstractDataSource
{
	public GigawordSource()
	{
		super(new GigawordIndex(), GigawordSource.class, Uri.LDC);
	}

	@Override
	protected String packageContent(String content)
	{
//		return new Data(Uri.LDC, content).asJson();
		return new Data(Uri.LIF, content).asJson();
	}

	@Override
	public String getMetadata()
	{
		if (metadata == null)
		{
			DataSourceMetadata md = getCommonMetadata();
			md.setName(this.getClass().getName());
			md.setDescription("English Gigaword text in a LAPPS container");
			md.addFormat(Uri.LDC);
			metadata = new Data<DataSourceMetadata>(Discriminators.Uri.META, md).asJson();
		}
		return metadata;
	}


}
