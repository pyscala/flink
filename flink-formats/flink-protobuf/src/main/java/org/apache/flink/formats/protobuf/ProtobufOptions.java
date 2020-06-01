package org.apache.flink.formats.protobuf;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

/**
 * @author liufangliang
 * @date 2020/6/1 5:31 PM
 */

public class ProtobufOptions {

	public static ConfigOption<Integer> PROTOBUF_VERSION = ConfigOptions.key("protobuf_version")
		.intType()
		.defaultValue(1)
		.withDescription("p ...");
}
