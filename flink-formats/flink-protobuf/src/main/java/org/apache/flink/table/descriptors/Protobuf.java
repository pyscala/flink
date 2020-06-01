package org.apache.flink.table.descriptors;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.utils.TypeStringUtils;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import java.util.Map;

import static org.apache.flink.table.descriptors.ProtobufValidator.*;

/**
 * @author liufangliang
 * @date 2020/5/29 3:47 PM
 */

public class Protobuf extends FormatDescriptor {


	private DescriptorProperties properties = new DescriptorProperties(true);

	public Protobuf() {
		super(FORMAT_TYPE_VALUE, 1);
	}

	public Protobuf setQttMessageVersion(String qttMessageVersion) {
		properties.putString(FORMAT_QUTOUTIAO_MESSAGE_VERSION, qttMessageVersion);
		return this;
	}

	public Protobuf setIgnoreParseErrors(boolean ignoreParseErrors) {
		properties.putBoolean(FORMAT_IGNORE_PARSE_ERRORS, ignoreParseErrors);
		return this;
	}

	public Protobuf setTypeInformation(TypeInformation<Row> typeInformation) {
		Preconditions.checkNotNull(typeInformation);
		properties.putString(FORMAT_SCHEMA, TypeStringUtils.writeTypeInfo(typeInformation));
		return this;
	}
	/**
	 * Converts this descriptor into a set of format properties. Usually prefixed with
	 * {@link FormatDescriptorValidator#FORMAT}.
	 */
	@Override
	protected Map<String, String> toFormatProperties() {
		return properties.asMap();
	}
}


