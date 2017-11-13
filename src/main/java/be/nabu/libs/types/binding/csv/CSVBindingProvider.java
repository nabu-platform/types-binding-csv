package be.nabu.libs.types.binding.csv;

import java.nio.charset.Charset;
import java.util.Collection;

import be.nabu.libs.property.api.Property;
import be.nabu.libs.property.api.Value;
import be.nabu.libs.types.api.ComplexType;
import be.nabu.libs.types.binding.api.BindingProvider;
import be.nabu.libs.types.binding.api.MarshallableBinding;
import be.nabu.libs.types.binding.api.UnmarshallableBinding;

public class CSVBindingProvider implements BindingProvider {

	@Override
	public String getContentType() {
		return "text/csv";
	}

	@Override
	public Collection<Property<?>> getSupportedProperties() {
		return null;
	}

	@Override
	public UnmarshallableBinding getUnmarshallableBinding(ComplexType type, Charset charset, Value<?>... values) {
		return new CSVBinding(type, charset);
	}

	@Override
	public MarshallableBinding getMarshallableBinding(ComplexType type, Charset charset, Value<?>... values) {
		return new CSVBinding(type, charset);
	}

}
