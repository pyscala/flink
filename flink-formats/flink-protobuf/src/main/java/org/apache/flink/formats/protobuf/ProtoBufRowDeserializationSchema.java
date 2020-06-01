package org.apache.flink.formats.protobuf;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import java.io.IOException;

/**
 * @author liufangliang
 * @date 2020/5/29 2:48 PM
 */

public class ProtoBufRowDeserializationSchema implements DeserializationSchema<Row> {


	/**
	 * Type information describing the result type.
	 */
	private final RowTypeInfo typeInfo;

	/**
	 * Schema version describing the protobuf message file
	 */
	private final String messageVersion;
	/**
	 * Flag indicating whether to ignore invalid fields/rows (default: throw an exception).
	 */
	private final boolean ignoreParseErrors;

	private ProtoBufRowDeserializationSchema(RowTypeInfo typeInfo, String messageVersion, boolean ignoreParseErrors) {
		this.typeInfo = typeInfo;
		this.messageVersion = messageVersion;
		this.ignoreParseErrors = ignoreParseErrors;
	}

	/**
	 * Gets the data type (as a {@link TypeInformation}) produced by this function or input format.
	 *
	 * @return The data type produced by this function or input format.
	 */
	@Override
	public TypeInformation<Row> getProducedType() {
		return typeInfo;
	}

	/**
	 * Deserializes the byte message.
	 *
	 * @param message The message, as a byte array.
	 * @return The deserialized message as an object (null if the message cannot be deserialized).
	 */
	@Override
	public Row deserialize(byte[] message) throws IOException {
		return null;
	}

	/**
	 * Method to decide whether the element signals the end of the stream. If
	 * true is returned the element won't be emitted.
	 *
	 * @param nextElement The element to test for the end-of-stream signal.
	 * @return True, if the element signals end of stream, false otherwise.
	 */
	@Override
	public boolean isEndOfStream(Row nextElement) {
		return false;
	}


	/**
	 * A builder for creating a {@link ProtoBufRowDeserializationSchema}
	 */
	@PublicEvolving
	public static class Builder {
		private RowTypeInfo typeInfo;
		private String messageVersion;
		private boolean ignoreParseErrors;

		public Builder() {
		}

		public Builder setTypeInfo(TypeInformation<Row> typeInfo) {
			Preconditions.checkNotNull(typeInfo, "Type information must not be null.");
			if (!(typeInfo instanceof RowTypeInfo)) {
				throw new IllegalArgumentException("Row type information expected.");
			}
			this.typeInfo = (RowTypeInfo) typeInfo;
			return this;
		}

		public Builder setMessageVersion(String messageVersion) {
			this.messageVersion = messageVersion;
			return this;
		}

		public Builder setIgnoreParseErrors(boolean ignoreParseErrors) {
			this.ignoreParseErrors = ignoreParseErrors;
			return this;
		}

		public ProtoBufRowDeserializationSchema build() {
			return new ProtoBufRowDeserializationSchema(typeInfo, messageVersion, ignoreParseErrors);
		}
	}
}
