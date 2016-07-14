package org.anc.lapps.gigaword;


import org.anc.index.api.Index;
import org.anc.io.UTF8Reader;
//import org.anc.lapps.oauth.database.Token;
//import org.anc.lapps.oauth.database.TokenDatabase;
import org.lappsgrid.api.DataSource;
import org.lappsgrid.metadata.DataSourceMetadata;
import org.lappsgrid.serialization.Data;
import org.lappsgrid.serialization.Error;
import org.lappsgrid.serialization.Serializer;
import static org.lappsgrid.discriminator.Discriminators.Uri;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import jp.go.nict.langrid.commons.net.URLUtil;
import jp.go.nict.langrid.commons.ws.ServiceContext;
import jp.go.nict.langrid.servicecontainer.handler.RIProcessor;
//import org.springframework.beans.factory.annotation.Autowired;

import javax.xml.soap.MimeHeader;
import javax.xml.soap.MimeHeaders;


/**
 * @author Keith Suderman
 */
public abstract class AbstractDataSource implements DataSource
{
	public static boolean testing = false;

	private final Logger logger;

	protected Index index;

	/** Metadata for the service is cached in this field so it does not need to be read for disk every request. */
	protected String metadata;
	/** The type of data returned by this service. */
	protected String returnType;
	/** The number of documents managed by this data source. */
	protected final int size;
	/** Error message set in the <tt>authenticate</tt> method. */
	private String errorMessage;

	public AbstractDataSource(Index index, Class<? extends AbstractDataSource> dsClass, String returnType)
	{
		this.index = index;
		this.returnType = returnType;
		this.logger = LoggerFactory.getLogger(dsClass);
		this.size = index.keys().size();
//		this.metadata = loadMetadata("metadata/" + dsClass.getName() + ".json");

	}

	public String execute(String input)
	{
		logger.debug("Executing request: {}", input);
		// Clear any existing error message.
		errorMessage = null;

		Map<String,Object> map = Serializer.parse(input, HashMap.class);
		String discriminator = (String) map.get("discriminator");
		if (discriminator == null)
		{
			logger.error("No discriminator present in request.");
			return new Error("No discriminator value provided.").asJson();
		}

		String result = null;
		switch (discriminator)
		{
			case Uri.SIZE:
				logger.debug("Fetching size");
				Data<Integer> sizeData = new Data<Integer>();
				Data d;

				sizeData.setDiscriminator(Uri.OK);
				sizeData.setPayload(size);
				result = Serializer.toJson(sizeData);
				break;
			case Uri.LIST:
				logger.debug("Fetching list");
				List<String> keys = index.keys();
				Map payload = (Map) map.get("payload");
				if (payload == null)
				{
					payload = new HashMap<String,String>();
				}
//				System.out.println("Payload is " + payload.getClass().getName());
				//System.out.println(payload.toString());
				//Map<String,Object> offsets = Serializer.parse(payload.toString(), Map.class);
				Object startValue = payload.get("start");
				if (startValue != null)
				{
					int start = 0;
					int offset = Integer.parseInt(startValue.toString());
					if (offset >= 0) {
						start = offset;
					}
					int end = index.keys().size();
					Object endValue = payload.get("end");
					if (endValue != null)
					{
						offset = Integer.parseInt(endValue.toString());
						if (offset >= start) {
							end = offset;
						}
					}
					logger.debug("Returning sublist {}-{}", start, end);
					keys = keys.subList(start, end);
				}
				Data<java.util.List<String>> listData = new Data<>();
				listData.setDiscriminator(Uri.STRING_LIST);
				listData.setPayload(keys);
				result = Serializer.toJson(listData);
				break;
			case Uri.GET:
				logger.debug("Fetching document");
				String key = map.get("payload").toString();
				if (key == null)
				{
					logger.error("No key value provided.");
					result = error("No key value provided");
				}
				else
				{
					File file = index.get(key);
					if (file == null)
					{
						result = error("No file for key " + key);
					}
					else if (!file.exists())
					{
						result = error("File not found: " + file.getPath());
					}
					else try
						{
							logger.debug("Loading text from file {}", file.getPath());
							UTF8Reader reader = new UTF8Reader(file);
							String content = reader.readString();
							reader.close();
//							result = packageContent(content);
							result = content;
						}
						catch (IOException e)
						{
							result = error(e.getMessage());
							logger.error("Error loading text for {}", file.getPath(),e);
						}

				}
				break;
			case Uri.GETMETADATA:
				logger.warn("Deprecated discriminator GETMETADATA used.");
				result = metadata;
				break;
			default:
				String message = String.format("Invalid discriminator: %s, Uri.List is %s", discriminator, Uri.LIST);
				//logger.warn(message);
				result = error(message);
				break;
		}
		logger.trace("Returning result {}", result);
		return result;
	}

	protected String packageContent(String content)
	{
		Data<String> data = new Data<String>(returnType, content);
		return data.asJson();
	}


	protected DataSourceMetadata getCommonMetadata()
	{
		DataSourceMetadata md = new DataSourceMetadata();
		md.setAllow(Uri.ANY);
		md.setEncoding("UTF-8");
		//TODO Configure and use the Version class again.
		md.setVersion(Version.getVersion());
		//md.setVersion("1.0.0");
		md.setVendor("http://www.anc.org");
		md.addLanguage("en-US");
		md.setLicense(Uri.CC_BY);
		return md;
	}

	protected String getToken(MimeHeaders headers)
	{
		String[] authorizations = headers.getHeader("Authorization");
		String token = checkAuthorizations(authorizations);
		if (token != null)
		{
			return token;
		}

		authorizations = headers.getHeader("X-Langrid-Service-Authorization");
		token = checkAuthorizations(authorizations);
		if (token != null)
		{
			return token;
		}
		logger.info("Unable to find an authorization header.");
		return null;
	}

	protected String checkAuthorizations(String[] authorizations)
	{
		if (authorizations == null || authorizations.length == 0)
		{
			return null;
		}
		for (String authorization : authorizations)
		{
			logger.debug("Checking authorization token: {}", authorization);
			if (authorization.startsWith("Bearer") || authorization.startsWith("bearer"))
			{
				return authorization.substring(7);
			}
		}
		return null;
	}

//	protected String loadMetadata(String metadataPath)
//	{
//		ClassLoader loader = this.getClass().getClassLoader();
////		System.out.println("Attempting to load metadata from " + metadataPath);
//		InputStream inputStream = loader.getResourceAsStream(metadataPath);
//		if (inputStream == null)
//		{
//			return error("Unable to locate metadata at: " + metadataPath);
//		}
//
//		String result;
//		try
//		{
//			UTF8Reader reader = new UTF8Reader(inputStream);
//			String json = reader.readString();
//			reader.close();
//			Data<String> data = new Data<String>();
//			data.setDiscriminator(Uri.META);
//			data.setPayload(json);
//			result = data.asJson();
//		}
//		catch (IOException e)
//		{
//			//return DataFactory.error("Unable to load metadata.", e);
//			result = new Error("Unable to load metadata.").asJson();
//		}
//		return result;
//	}

	protected String error(String message, Throwable t)
	{
		logger.error(message, t);
		return new Error(message).asJson();
	}

	protected String error(String message)
	{
		logger.error(message);
		return new Error(message).asJson();
	}
}
