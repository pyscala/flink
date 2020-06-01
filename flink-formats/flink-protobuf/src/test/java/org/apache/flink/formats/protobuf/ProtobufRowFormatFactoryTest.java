package org.apache.flink.formats.protobuf;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.descriptors.Protobuf;
import org.apache.flink.table.factories.DeserializationSchemaFactory;
import org.apache.flink.table.factories.TableFactoryService;
import org.apache.flink.types.Row;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Created by liufangliang on 2020/6/1.
 */
public class ProtobufRowFormatFactoryTest {
	@Test
	public void createSerializationSchema() throws Exception {
	}

	@Test
	public void createDeserializationSchema() throws Exception {
		final String messageVersion = "message-v1";
		final Boolean ignoreParseErrors = false;

		final TypeInformation<Row> information = Types.ROW_NAMED(new String[]{"fieldname1", "fieldname2"}
			, new TypeInformation[]{Types.STRING, Types.STRING});
		final Protobuf protobuf = new Protobuf()
			.setIgnoreParseErrors(ignoreParseErrors)
			.setQttMessageVersion(messageVersion)
			.setTypeInformation(information);

		final DeserializationSchema deserializationSchema = TableFactoryService
			.find(DeserializationSchemaFactory.class, protobuf.toProperties())
			.createDeserializationSchema(protobuf.toProperties());

		final ProtoBufRowDeserializationSchema build = new ProtoBufRowDeserializationSchema
			.Builder()
			.setTypeInfo(information)
			.setIgnoreParseErrors(ignoreParseErrors)
			.setMessageVersion(messageVersion)
			.build();

		assertEquals(deserializationSchema, build);

	}

}
