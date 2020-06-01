package org.apache.flink.table.descriptors;

import java.util.Arrays;

/**
 * @author liufangliang
 * @date 2020/5/29 3:49 PM
 */

public class ProtobufValidator extends FormatDescriptorValidator {
	public static final String FORMAT_TYPE_VALUE = "protobuf";
	public static final String FORMAT_SCHEMA = "format.schema";
	public static final String FORMAT_QUTOUTIAO_MESSAGE_VERSION = "format.qutoutiao-message-version";
	public static final String FORMAT_IGNORE_PARSE_ERRORS = "format.ignore-parse-errors";

	@Override
	public void validate(DescriptorProperties properties) {
		super.validate(properties);
		properties.validateBoolean(FORMAT_IGNORE_PARSE_ERRORS,true);
		properties.validateEnumValues(FORMAT_QUTOUTIAO_MESSAGE_VERSION,false, Arrays.asList("Message-v1","Message-v2","Message-v3"));
	}
}
