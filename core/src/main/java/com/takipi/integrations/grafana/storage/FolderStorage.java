package com.takipi.integrations.grafana.storage;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;

public class FolderStorage extends BaseStorage {
			
	private final String folder;
	
	public FolderStorage(String folder) {
		this.folder = folder;
	}
	
	private File getFile(String name) {
		
		String settingsFolder = System.getProperty(folder);	
		
		if (settingsFolder == null) {
			return null;
		}
		
		File folder = new File(settingsFolder);		
		File file = new File(folder, name);

		return file;
	}
	
	@Override
	protected InputStream getInputStream(String key) {
				
		try {
			File file = getFile(key);
			
			if ((file == null) || (!file.exists())) {
				return null;
			}
			
			return new FileInputStream(file);
		} catch (FileNotFoundException e) {
			throw new IllegalStateException(e);
		}
	}

	@Override
	protected OutputStream getOutputStream(String key) {
				
		try {
			File file = getFile(key);
			
			if (file == null) {
				return null;
			}
			
			if (!file.getParentFile().exists()) {
				file.getParentFile().mkdirs();
			}
			
			return new FileOutputStream(file);
		} catch (FileNotFoundException e) {
			throw new IllegalStateException(e);
		}
	}
}
