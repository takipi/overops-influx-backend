package com.takipi.integrations.grafana.settings;

public class SettingsVersion implements Comparable<SettingsVersion> {
	public static final SettingsVersion NO_VERSION = of(0, 0, 0);

	private static final String DELIM = ".";
	private static final String DELIM_REGEX = "\\.";

	private final int major;
	private final int minor;
	private final int maintenance;

	private SettingsVersion(int major, int minor, int maintenance) {
		this.major = major;
		this.minor = minor;
		this.maintenance = maintenance;
	}

	public int getMajor() {
		return major;
	}

	public int getMinor() {
		return minor;
	}

	public int getMaintenance() {
		return maintenance;
	}

	@Override
	public boolean equals(Object o) {
		if ((o == null) || !(o instanceof SettingsVersion)) {
			return false;
		}

		SettingsVersion tv = (SettingsVersion) o;

		return ((major == tv.major) && (minor == tv.minor) && (maintenance == tv.maintenance));
	}

	@Override
	public int hashCode() {
		return (major + minor * 31 + maintenance * 259);
	}

	@Override
	public int compareTo(SettingsVersion o) {
		if (major < o.major)
			return -1;
		if (major > o.major)
			return 1;

		if (minor < o.minor)
			return -1;
		if (minor > o.minor)
			return 1;

		if (maintenance < o.maintenance)
			return -1;
		if (maintenance > o.maintenance)
			return 1;

		return 0;
	}

	public boolean isOlderThan(SettingsVersion o) {
		return ((o != null) && (compareTo(o) < 0));
	}

	public boolean isOlderOrEqual(SettingsVersion o) {
		return ((o != null) && (compareTo(o) <= 0));
	}

	public boolean isNewerThan(SettingsVersion o) {
		return ((o != null) && (compareTo(o) > 0));
	}

	public boolean isNewerOrEqual(SettingsVersion o) {
		return ((o != null) && (compareTo(o) >= 0));
	}

	@Override
	public String toString() {
		return (major + DELIM + minor + DELIM + maintenance);
	}

	public static SettingsVersion of(int major, int minor, int maintenance) {
		return new SettingsVersion(major, minor, maintenance);
	}

	public static SettingsVersion parse(String str) {
		String[] parts = str.split(DELIM_REGEX);

		if (parts.length != 3) {
			throw new IllegalArgumentException(
					"Invalid version format, 3 parts expected, " + parts.length + " received: \"" + str + "\"");
		}

		int major;
		int minor;
		int maintenance;

		try {
			major = Integer.parseInt(parts[0]);
			minor = Integer.parseInt(parts[1]);
			maintenance = Integer.parseInt(parts[2]);
		} catch (NumberFormatException e) {
			throw new IllegalArgumentException("All version parts must be integral", e);
		}

		return of(major, minor, maintenance);
	}

	public static SettingsVersion safeParse(String str) {
		return safeParse(str, NO_VERSION);
	}

	public static SettingsVersion safeParse(String str, SettingsVersion defaultValue) {
		if ((str == null) || (str.isEmpty())) {
			return defaultValue;
		}

		try {
			return parse(str);
		} catch (Exception e) {
			return defaultValue;
		}
	}
}
