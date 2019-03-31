package com.takipi.integrations.grafana.storage;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.StringWriter;
import java.nio.charset.Charset;

import org.apache.commons.io.IOUtils;

import com.takipi.integrations.grafana.settings.GrafanaSettings;
import com.takipi.integrations.grafana.settings.SettingsStorage;

public abstract class BaseStorage implements SettingsStorage {
	
	@Override
	public String getDefaultServiceSettings() {
		return getValue(GrafanaSettings.DEFAULT);
	}

	@Override
	public void setValue(String name, String settings) {
		
		try {
			OutputStream outputStream = getOutputStream(name);
			
			if (outputStream == null) {
				throw new IllegalStateException("Can not store settings for " + name);
			}
			
			IOUtils.write(settings, outputStream, Charset.defaultCharset());
			outputStream.close();
		} catch (IOException e) {
			throw new IllegalStateException(e);
		}
	}
	
	@Override
	public String getValue(String name) {
		
		try {
			InputStream inputStream = getInputStream(name);
			
			if (inputStream == null) {
				return null;
			}
			
			StringWriter writer = new StringWriter();
			IOUtils.copy(inputStream, writer, Charset.defaultCharset());
			inputStream.close();
			String result = writer.toString();
			return result;
		} catch (IOException e) {
			throw new IllegalStateException(e);
		}
	}
	
	protected abstract InputStream getInputStream(String key);
	protected abstract OutputStream getOutputStream(String key);
}
