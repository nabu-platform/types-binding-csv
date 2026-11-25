package be.nabu.libs.types.binding.csv;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.xml.bind.annotation.XmlType;

import be.nabu.libs.types.TypeUtils;
import be.nabu.libs.types.api.ComplexContent;
import be.nabu.libs.types.api.ComplexType;
import be.nabu.libs.types.api.DefinedType;
import be.nabu.libs.types.binding.api.Window;
import be.nabu.libs.types.java.BeanInstance;
import be.nabu.libs.types.java.BeanResolver;
import junit.framework.TestCase;

/**
 * Marshalling must produce RFC 4180 CSV: a value that contains the field separator, the quote character or a line
 * break (CR or LF, not only the configured record separator) is quoted, and a quote inside it is doubled. Before this,
 * a value with a quote was written unescaped, and a value with a lone LF under a CRLF record separator was not quoted
 * at all, so spreadsheet programs split those rows wrongly.
 */
public class TestCSVMarshal extends TestCase {

	private static final Charset UTF8 = Charset.forName("UTF-8");

	private String marshal(CSVBinding binding, Lines lines) throws IOException {
		ByteArrayOutputStream output = new ByteArrayOutputStream();
		binding.marshal(output, new BeanInstance<Lines>(lines));
		return new String(output.toByteArray(), UTF8);
	}

	private CSVBinding binding() {
		DefinedType type = BeanResolver.getInstance().resolve(Lines.class);
		CSVBinding binding = new CSVBinding((ComplexType) type, UTF8);
		binding.setFieldSeparator(";");
		binding.setRecordSeparator("\r\n");
		return binding;
	}

	private static Lines lines(Line...lines) {
		Lines result = new Lines();
		result.setLines(new ArrayList<Line>(Arrays.asList(lines)));
		return result;
	}

	private static Line line(String name, String city, String note) {
		Line line = new Line();
		line.setName(name);
		line.setCity(city);
		line.setNote(note);
		return line;
	}

	public void testQuotesAreDoubled() throws IOException {
		CSVBinding binding = binding();
		binding.setUseHeader(false);
		String csv = marshal(binding, lines(line("Bakkerij \"De Korst\"; Gent", "say \"hi\"", "plain")));
		assertEquals("\"Bakkerij \"\"De Korst\"\"; Gent\";\"say \"\"hi\"\"\";plain\r\n", csv);
	}

	public void testAnyLineBreakIsQuoted() throws IOException {
		CSVBinding binding = binding();
		binding.setUseHeader(false);
		String csv = marshal(binding, lines(line("first\nsecond", "carriage\rreturn", "both\r\nends")));
		assertEquals("\"first\nsecond\";\"carriage\rreturn\";\"both\r\nends\"\r\n", csv);
	}

	public void testRoundTrip() throws IOException, ParseException {
		CSVBinding binding = binding();
		binding.setUseHeader(true);
		String csv = marshal(binding, lines(line("a\"b;c", "x;y", "\"quoted\"")));
		ComplexContent parsed = binding.unmarshal(new ByteArrayInputStream(csv.getBytes(UTF8)), new Window[0]);
		Lines back = TypeUtils.getAsBean(parsed, Lines.class);
		assertEquals("a\"b;c", back.getLines().get(0).getName());
		assertEquals("x;y", back.getLines().get(0).getCity());
		assertEquals("\"quoted\"", back.getLines().get(0).getNote());
	}

	public void testByteOrderMark() throws IOException {
		CSVBinding binding = binding();
		binding.setUseHeader(false);
		binding.setByteOrderMark(true);
		String csv = marshal(binding, lines(line("a", "b", "c")));
		assertEquals("﻿a;b;c\r\n", csv);
		binding.setByteOrderMark(false);
		assertEquals("a;b;c\r\n", marshal(binding, lines(line("a", "b", "c"))));
	}

	public void testSelectedFieldsAndHeaders() throws IOException {
		CSVBinding binding = binding();
		binding.setUseHeader(true);
		binding.setFields(Arrays.asList("note", "name"));
		binding.setHeaders(Arrays.asList("Opmerking", "Naam; volledig"));
		String csv = marshal(binding, lines(line("Jan", "Gent", "ok")));
		assertEquals("Opmerking;\"Naam; volledig\"\r\nok;Jan\r\n", csv);
	}

	public void testUnknownFieldIsRefused() throws IOException {
		CSVBinding binding = binding();
		binding.setFields(Arrays.asList("name", "passwordHash"));
		try {
			marshal(binding, lines(line("Jan", "Gent", "ok")));
			fail("an unknown field must be refused, not silently skipped");
		}
		catch (IllegalArgumentException e) {
			assertTrue(e.getMessage().contains("passwordHash"));
		}
	}

	public void testHeadersMustMatchFields() throws IOException {
		CSVBinding binding = binding();
		binding.setFields(Arrays.asList("name", "city"));
		binding.setHeaders(Arrays.asList("Naam"));
		try {
			marshal(binding, lines(line("Jan", "Gent", "ok")));
			fail("headers and fields of a different length must be refused");
		}
		catch (IllegalArgumentException e) {
			assertTrue(e.getMessage().contains("headers"));
		}
	}

	public static class Lines {
		private List<Line> lines;
		public List<Line> getLines() {
			return lines;
		}
		public void setLines(List<Line> lines) {
			this.lines = lines;
		}
	}

	@XmlType(propOrder = {"name", "city", "note"})
	public static class Line {
		private String name, city, note;
		public String getName() {
			return name;
		}
		public void setName(String name) {
			this.name = name;
		}
		public String getCity() {
			return city;
		}
		public void setCity(String city) {
			this.city = city;
		}
		public String getNote() {
			return note;
		}
		public void setNote(String note) {
			this.note = note;
		}
	}
}
