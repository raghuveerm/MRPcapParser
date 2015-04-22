package com.calsoftlabs.location;

import java.io.File;
import java.io.IOException;

import com.maxmind.geoip.Location;
import com.maxmind.geoip.LookupService;

public class GeoLocation {

	private static final File FILE = new File("/var/data/GeoLiteCity.dat");

	public String getLocation(String ipAddress) throws IOException,
			NullPointerException {

		String data;
		String city;
		String code;

		LookupService lookup = new LookupService(FILE,
				LookupService.GEOIP_MEMORY_CACHE);
		Location locationServices = lookup.getLocation(ipAddress);

		if (locationServices != null) {
			city = locationServices.countryName;
			code = locationServices.countryCode;
		} else {
			city = "Unknown";
			code = "UN";
		}

		data = city + "," + code;

		return data;
	}
}
