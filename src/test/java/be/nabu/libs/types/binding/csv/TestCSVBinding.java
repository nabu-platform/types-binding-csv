package be.nabu.libs.types.binding.csv;

import java.io.IOException;
import java.nio.charset.Charset;
import java.text.ParseException;
import java.util.List;

import javax.xml.bind.annotation.XmlType;

import be.nabu.libs.types.TypeUtils;
import be.nabu.libs.types.api.ComplexType;
import be.nabu.libs.types.api.DefinedType;
import be.nabu.libs.types.binding.api.Window;
import be.nabu.libs.types.java.BeanResolver;
import junit.framework.TestCase;

public class TestCSVBinding extends TestCase {
	
	public void testParse() throws IOException, ParseException {
		DefinedType definedType = BeanResolver.getInstance().resolve(TestRecords.class);
		CSVBinding binding = new CSVBinding((ComplexType) definedType, Charset.defaultCharset());
		binding.setUseHeader(true);
		binding.setValidateHeader(false);
		TestRecords test = TypeUtils.getAsBean(binding.unmarshal(Thread.currentThread().getContextClassLoader().getResourceAsStream("example.csv"), new Window[0]), TestRecords.class);
		assertEquals(",GIL VINCENTE", test.getRecords().get(0).getStreetNl());
		assertEquals("Industriepark \"De Bruwaan\"", test.getRecords().get(1).getStreetNl());
		
	}
	
	public static class TestRecords {
		private List<TestRecord> records;

		public List<TestRecord> getRecords() {
			return records;
		}
		public void setRecords(List<TestRecord> records) {
			this.records = records;
		}
	}
	
	@XmlType(propOrder = {"entityNumber", "typeOfAddress", "countryNl", "countryFr", "zipCode", "municipalityNl", "municipalityFr", "streetNl", "streetFr", "houseNumber", "box", "extraAddressInfo", "dateStrikingOff"})
	public static class TestRecord {
		private String entityNumber, typeOfAddress, countryNl, countryFr, zipCode, municipalityNl, municipalityFr, streetNl, streetFr, houseNumber, box, extraAddressInfo, dateStrikingOff;

		public String getEntityNumber() {
			return entityNumber;
		}

		public void setEntityNumber(String entityNumber) {
			this.entityNumber = entityNumber;
		}

		public String getTypeOfAddress() {
			return typeOfAddress;
		}

		public void setTypeOfAddress(String typeOfAddress) {
			this.typeOfAddress = typeOfAddress;
		}

		public String getCountryNl() {
			return countryNl;
		}

		public void setCountryNl(String countryNl) {
			this.countryNl = countryNl;
		}

		public String getCountryFr() {
			return countryFr;
		}

		public void setCountryFr(String countryFr) {
			this.countryFr = countryFr;
		}

		public String getZipCode() {
			return zipCode;
		}

		public void setZipCode(String zipCode) {
			this.zipCode = zipCode;
		}

		public String getMunicipalityNl() {
			return municipalityNl;
		}

		public void setMunicipalityNl(String municipalityNl) {
			this.municipalityNl = municipalityNl;
		}

		public String getMunicipalityFr() {
			return municipalityFr;
		}

		public void setMunicipalityFr(String municipalityFr) {
			this.municipalityFr = municipalityFr;
		}

		public String getStreetNl() {
			return streetNl;
		}

		public void setStreetNl(String streetNl) {
			this.streetNl = streetNl;
		}

		public String getStreetFr() {
			return streetFr;
		}

		public void setStreetFr(String streetFr) {
			this.streetFr = streetFr;
		}

		public String getHouseNumber() {
			return houseNumber;
		}

		public void setHouseNumber(String houseNumber) {
			this.houseNumber = houseNumber;
		}

		public String getBox() {
			return box;
		}

		public void setBox(String box) {
			this.box = box;
		}

		public String getExtraAddressInfo() {
			return extraAddressInfo;
		}

		public void setExtraAddressInfo(String extraAddressInfo) {
			this.extraAddressInfo = extraAddressInfo;
		}

		public String getDateStrikingOff() {
			return dateStrikingOff;
		}

		public void setDateStrikingOff(String dateStrikingOff) {
			this.dateStrikingOff = dateStrikingOff;
		}
	}
}
