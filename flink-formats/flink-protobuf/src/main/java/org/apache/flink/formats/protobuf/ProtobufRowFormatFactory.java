package org.apache.flink.formats.protobuf;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.descriptors.ProtobufValidator;
import org.apache.flink.table.factories.*;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.descriptors.ProtobufValidator.*;

/**
 * @author liufangliang
 * @date 2020/5/29 2:57 PM
 */

public class ProtobufRowFormatFactory extends TableFormatFactoryBase<Row> implements DeserializationSchemaFactory<Row>, SerializationSchemaFactory<Row> {

	/**
	 * Creates and configures a [[SerializationSchema]] using the given properties.
	 *
	 * @param properties normalized properties describing the format
	 * @return the configured serialization schema or null if the factory cannot provide an
	 * instance of this class
	 */
	@Override
	public SerializationSchema<Row> createSerializationSchema(Map<String, String> properties) {
		return null;
	}

	/**
	 * Creates and configures a {@link DeserializationSchema} using the given properties.
	 *
	 * @param properties normalized properties describing the format
	 * @return the configured serialization schema or null if the factory cannot provide an
	 * instance of this class
	 */
	@Override
	public DeserializationSchema<Row> createDeserializationSchema(Map<String, String> properties) {
		final DescriptorProperties descriptorProperties = getValidatedProperties(properties);
		descriptorProperties.putProperties(properties);
		ProtoBufRowDeserializationSchema.Builder builder = new ProtoBufRowDeserializationSchema.Builder();
		builder.setTypeInfo(createTypeInformation(descriptorProperties));
		descriptorProperties.getOptionalBoolean(FORMAT_IGNORE_PARSE_ERRORS).ifPresent(builder::setIgnoreParseErrors);
		descriptorProperties.getOptionalString(FORMAT_QUTOUTIAO_MESSAGE_VERSION).ifPresent(builder::setMessageVersion);
		return builder.build();
	}

	public ProtobufRowFormatFactory() {
		super(FORMAT_TYPE_VALUE, 1, true);
	}

	/**
	 * Format specific context.
	 * <p>
	 * <p>This method can be used if format type and a property version is not enough.
	 */
	@Override
	protected Map<String, String> requiredFormatContext() {
		return super.requiredFormatContext();
	}

	/**
	 * Format specific supported properties.
	 * <p>
	 * <p>This method can be used if schema derivation is not enough.
	 */
	@Override
	protected List<String> supportedFormatProperties() {
		final List<String> properties = new ArrayList<>();
		properties.add(ProtobufValidator.FORMAT_QUTOUTIAO_MESSAGE_VERSION);
		properties.add(ProtobufValidator.FORMAT_IGNORE_PARSE_ERRORS);
		return properties;
	}

	private TypeInformation<Row> createTypeInformation(DescriptorProperties properties) {
		return deriveSchema(properties.asMap()).toRowType();
	}

	private static DescriptorProperties getValidatedProperties(Map<String, String> propertiesMap) {
		final DescriptorProperties descriptorProperties = new DescriptorProperties();
		descriptorProperties.putProperties(propertiesMap);
		// validate
		new ProtobufValidator().validate(descriptorProperties);

		return descriptorProperties;
	}

}
