
package com.takipi.integrations.grafana.settings;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.StringWriter;
import java.nio.charset.Charset;

import org.apache.commons.io.IOUtils;

import com.takipi.api.client.ApiClient;

public abstract class BaseSettingsStorage implements SettingsStorage {
	
	protected abstract InputStream getInputStream(String key);
	protected abstract OutputStream getOutputStream(String key);
	
	private String getSettings(String key) {

		try {
			InputStream inputStream = getInputStream(key);
			
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

	@Override
	public String getDefaultServiceSettings() {
		return getSettings(GrafanaSettings.DEFAULT);
	}

	@Override
	public String getServiceSettings(String name) {
		return getSettings(name);
	}

	@Override
	public void saveServiceSettings(String name, String settings) {

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
}
