package be.nabu.libs.types.binding.csv;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import be.nabu.libs.converter.ConverterFactory;
import be.nabu.libs.property.api.Value;
import be.nabu.libs.resources.api.ReadableResource;
import be.nabu.libs.types.CollectionHandlerFactory;
import be.nabu.libs.types.ComplexContentWrapperFactory;
import be.nabu.libs.types.TypeUtils;
import be.nabu.libs.types.api.CollectionHandlerProvider;
import be.nabu.libs.types.api.ComplexContent;
import be.nabu.libs.types.api.ComplexType;
import be.nabu.libs.types.api.Element;
import be.nabu.libs.types.api.Marshallable;
import be.nabu.libs.types.api.SimpleType;
import be.nabu.libs.types.binding.BaseTypeBinding;
import be.nabu.libs.types.binding.api.PartialUnmarshaller;
import be.nabu.libs.types.binding.api.Window;
import be.nabu.libs.types.binding.api.WindowedList;
import be.nabu.utils.io.IOUtils;
import be.nabu.utils.io.api.ByteBuffer;
import be.nabu.utils.io.api.CharBuffer;
import be.nabu.utils.io.api.ReadableContainer;
import be.nabu.utils.io.api.WritableContainer;
import be.nabu.utils.io.containers.LimitedMarkableContainer;
import be.nabu.utils.io.containers.chars.BackedDelimitedCharContainer;

public class CSVBinding extends BaseTypeBinding {

	private Charset charset;
	private ComplexType type;
	private String fieldSeparator = ",";
	private String recordSeparator = "\n";
	private String quoteCharacter = "\"";
	private long lookAhead = 409600;
	private boolean useHeader = true;
	private boolean trim = false;
	private ReadableResource resource;

	public CSVBinding(ComplexType type, Charset charset) {
		this.type = type;
		this.charset = charset;
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public void marshal(OutputStream output, ComplexContent content, Value<?>...values) throws IOException {
		WritableContainer<CharBuffer> writable = IOUtils.wrapWritable(IOUtils.wrap(output), charset);
		
		for (Element<?> element : TypeUtils.getAllChildren(content.getType())) {
			if (element.getType() instanceof ComplexType && element.getType().isList(element.getProperties())) {
				Collection<Element<?>> children = TypeUtils.getAllChildren((ComplexType) element.getType());

				if (useHeader) {
					boolean first = true;
					for (Element<?> child : children) {
						if (!(child.getType() instanceof SimpleType)) {
							continue;
						}
						writable.write(IOUtils.wrap((first ? "" : fieldSeparator) + child.getName()));
						first = false;
					}
					writable.write(IOUtils.wrap(recordSeparator));
				}
				
				Object object = content.get(element.getName());
				// no data, skip
				if (object == null) {
					continue;
				}
				CollectionHandlerProvider handler = CollectionHandlerFactory.getInstance().getHandler().getHandler(object.getClass());
				for (Object item : handler.getAsCollection(object)) {
					if (!(item instanceof ComplexContent)) {
						item = ComplexContentWrapperFactory.getInstance().getWrapper().wrap(item);
					}
					boolean first = true;
					for (Element<?> child : children) {
						if (!(child.getType() instanceof SimpleType)) {
							continue;
						}
						Object value = ((ComplexContent) item).get(child.getName());
						if (!(value instanceof String)) {
							if (child.getType() instanceof Marshallable) {
								value = ((Marshallable) child.getType()).marshal(value, child.getProperties());
							}
							else {
								value = ConverterFactory.getInstance().getConverter().convert(value, String.class);
							}
						}
						if (((String) value).contains(fieldSeparator) || ((String) value).contains(recordSeparator)) {
							value = quoteCharacter + value + quoteCharacter;
						}
						writable.write(IOUtils.wrap((first ? "" : fieldSeparator) + value));
						first = false;
					}
					writable.write(IOUtils.wrap(recordSeparator));
				}
			}
		}
	}

	@Override
	protected ComplexContent unmarshal(ReadableResource resource, Window[] windows, Value<?>...values) throws IOException, ParseException {
		this.resource = resource;
		ReadableContainer<ByteBuffer> bytes = IOUtils.bufferReadable(resource.getReadable(), IOUtils.newByteBuffer(409600, true));
		ReadableContainer<CharBuffer> chars = IOUtils.wrapReadable(bytes, charset);
		
		ComplexContent instance = type.newInstance();
		
		LimitedMarkableContainer<CharBuffer> marked = new LimitedMarkableContainer<CharBuffer>(IOUtils.bufferReadable(chars, IOUtils.newCharBuffer(102400, true)), lookAhead);
		marked.mark();
		unmarshal(type.getName(), marked, instance, true, 0, windows);
		return instance;
	}
	
	private Element<?> getElementFor(ComplexType type, String...parts) {
		for (Element<?> element : TypeUtils.getAllChildren(type)) {
			if (element.getType() instanceof ComplexType) {
				Collection<Element<?>> children = TypeUtils.getAllChildren((ComplexType) element.getType());
				if (children.size() == parts.length) {
					return element;
				}
			}
		}
		throw new IllegalArgumentException("No element found that matches the given parts: " + Arrays.asList(parts));
	}
	
	public class PartialCSVUnmarshaller implements PartialUnmarshaller {

		private String path;
		private Element<?> element;

		public PartialCSVUnmarshaller(String path, Element<?> element) {
			this.path = path;
			this.element = element;
		}
		
		@SuppressWarnings({ "unchecked", "rawtypes" })
		@Override
		public List<ComplexContent> unmarshal(InputStream input, long offset, int batchSize) throws IOException, ParseException {
			// IMPORTANT: without the byte buffer there is _massive_ performance loss
			ReadableContainer<CharBuffer> readable = IOUtils.wrapReadable(IOUtils.wrap(input), charset);
			long skipChars = IOUtils.skipChars(readable, offset);
			if (skipChars != offset) {
				throw new IOException("Could not skip to position " + offset + ", stopped at: " + skipChars);
			}
			LimitedMarkableContainer<CharBuffer> marked = new LimitedMarkableContainer<CharBuffer>(IOUtils.bufferReadable(readable, IOUtils.newCharBuffer(102400, true)), lookAhead);
			marked.mark();
			ComplexContent newInstance = type.newInstance();
			// there is no nested windowing in csv and the current window must not activate
			CSVBinding.this.unmarshal(path, marked, newInstance, false, batchSize);
			Object object = newInstance.get(element.getName());
			if (object == null) {
				return null;
			}
			else if (object instanceof List) {
				return (List<ComplexContent>) object;
			}
			else if (object instanceof Collection) {
				return new ArrayList<ComplexContent>((List) object);
			}
			else {
				List<ComplexContent> entries = new ArrayList<ComplexContent>();
				CollectionHandlerProvider handler = CollectionHandlerFactory.getInstance().getHandler().getHandler(object.getClass());
				entries.addAll(handler.getAsCollection(object));
				return entries;
			}
		}
		
	}

	@SuppressWarnings("rawtypes")
	private void unmarshal(String path, LimitedMarkableContainer<CharBuffer> marked, ComplexContent parent, boolean isFullParse, int maxSize, Window...windows) throws IOException {
		Element<?> previousElement = null;
		
		Map<String, Integer> indexes = new HashMap<String, Integer>();
		long totalRead = 0;
		int recordCount = 0;
		while (!Thread.interrupted()) {
			// get the record
			BackedDelimitedCharContainer delimited = new BackedDelimitedCharContainer(marked, 4096, recordSeparator);
			String record = IOUtils.toString(delimited);
			int read = record.length() + (delimited.getMatchedDelimiter() == null ? 0 : delimited.getMatchedDelimiter().length());
			totalRead += read;
			marked.moveMarkAbsolute(totalRead);

			String remainder = delimited.getRemainder();
			if (remainder != null && !remainder.isEmpty()) {
				marked.pushback(IOUtils.wrap(remainder));
			}

			// skip empty lines
			if (record.trim().isEmpty()) {
				// if we found a delimiter, it is an intentionally empty line
				if (delimited.isDelimiterFound()) {
					continue;
				}
				// otherwise, we have reached the end!
				else {
					break;
				}
			}
			
			// the minus 1 at the end makes sure we get empty strings as well, split() will (by default) drop empty strings at the end
			String [] parts = record.split(fieldSeparator, -1);
			Element<?> element = getElementFor(type, parts);
			
			// if we change type, we might need to stop
			if (!isFullParse && previousElement != null && !previousElement.equals(element)) {
				break;
			}
			
			// we begin a new type
			if (previousElement == null || !previousElement.equals(element)) {
				recordCount = 0;
				previousElement = element;
				// we assume the first record to be a header
				if (useHeader && isFullParse) {
					continue;
				}
			}
			
			ComplexContent content = ((ComplexType) element.getType()).newInstance();
			int field = 0;
			for (Element<?> child : TypeUtils.getAllChildren(content.getType())) {
				String value = parts[field++];
				if (value.startsWith(quoteCharacter) && value.endsWith(quoteCharacter)) {
					value = value.substring(quoteCharacter.length(), value.length() - quoteCharacter.length());
				}
				if (trim) {
					value = value.trim();
				}
				if (!value.isEmpty()) {
					content.set(child.getName(), value);
				}
			}
			
			int index;
			if (!indexes.containsKey(element.getName())) {
				index = 0;
			}
			else {
				index = indexes.get(element.getName()) + 1;
			}
			
			indexes.put(element.getName(), index);

			Window activeWindow = null;
			for (Window window : windows) {
				if (window.getPath().equals(path + "/" + element.getName()) || window.getPath().equals(element.getName())) {
					activeWindow = window;
					break;
				}
			}

			if (activeWindow != null) {
				Object current = parent.get(element.getName());
				WindowedList list = null;
				if (current instanceof WindowedList) {
					list = (WindowedList) current;
				}
				else if (current == null || (current instanceof Collection && ((Collection) current).size() == 0)) {
					list = new WindowedList(resource, activeWindow, new PartialCSVUnmarshaller(path, element));
					parent.set(element.getName(), list);
				}
				else {
					throw new IllegalStateException("Can not start windowed list if a non-windowed is already set: " + current);
				}
				// always register the offset
				list.setOffset(index, totalRead - read);
				// only register the object if it is within the window size
				if (index < activeWindow.getSize()) {
					parent.set(element.getName() + "[" + index + "]", content);
				}
			}
			else {
				parent.set(element.getName() + "[" + index + "]", content);
			}
			
			previousElement = element;

			recordCount++;
			// if the record delimiter was not found, we are at the end of the file
			if (!delimited.isDelimiterFound()) {
				break;
			}
			// we have reached the amount we wanted
			else if (maxSize > 0 && recordCount > maxSize) {
				break;
			}
		}
	}

	public String getRecordSeparator() {
		return recordSeparator;
	}

	public void setRecordSeparator(String recordSeparator) {
		this.recordSeparator = recordSeparator;
	}

	public String getQuoteCharacter() {
		return quoteCharacter;
	}

	public void setQuoteCharacter(String quoteCharacter) {
		this.quoteCharacter = quoteCharacter;
	}

	public boolean isUseHeader() {
		return useHeader;
	}

	public void setUseHeader(boolean useHeader) {
		this.useHeader = useHeader;
	}

	public String getFieldSeparator() {
		return fieldSeparator;
	}

	public void setFieldSeparator(String fieldSeparator) {
		this.fieldSeparator = fieldSeparator;
	}

	public boolean isTrim() {
		return trim;
	}

	public void setTrim(boolean trim) {
		this.trim = trim;
	}
	
}
