/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */
package io.airbyte.integrations.destination.mysql.typing_deduping

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode
import io.airbyte.cdk.db.jdbc.DefaultJdbcDatabase
import io.airbyte.cdk.db.jdbc.JdbcDatabase
import io.airbyte.cdk.db.jdbc.JdbcSourceOperations
import io.airbyte.cdk.integrations.base.JavaBaseConstants.COLUMN_NAME_AB_EXTRACTED_AT
import io.airbyte.cdk.integrations.base.JavaBaseConstants.COLUMN_NAME_AB_ID
import io.airbyte.cdk.integrations.base.JavaBaseConstants.COLUMN_NAME_AB_LOADED_AT
import io.airbyte.cdk.integrations.base.JavaBaseConstants.COLUMN_NAME_AB_META
import io.airbyte.cdk.integrations.base.JavaBaseConstants.COLUMN_NAME_AB_RAW_ID
import io.airbyte.cdk.integrations.base.JavaBaseConstants.COLUMN_NAME_DATA
import io.airbyte.cdk.integrations.base.JavaBaseConstants.COLUMN_NAME_EMITTED_AT
import io.airbyte.cdk.integrations.destination.jdbc.typing_deduping.JdbcSqlGenerator
import io.airbyte.cdk.integrations.standardtest.destination.typing_deduping.JdbcSqlGeneratorIntegrationTest
import io.airbyte.commons.json.Jsons
import io.airbyte.integrations.base.destination.typing_deduping.DestinationHandler
import io.airbyte.integrations.base.destination.typing_deduping.StreamId
import io.airbyte.integrations.base.destination.typing_deduping.migrators.MinimumDestinationState
import io.airbyte.integrations.destination.mysql.MySQLDestination
import io.airbyte.integrations.destination.mysql.MySQLDestinationAcceptanceTest
import io.airbyte.integrations.destination.mysql.MySQLNameTransformer
import io.airbyte.integrations.destination.mysql.typing_deduping.MysqlSqlGenerator.Companion.TIMESTAMP_FORMATTER
import java.sql.ResultSet
import java.sql.SQLException
import java.time.OffsetDateTime
import java.util.Locale
import javax.sql.DataSource
import org.jooq.DataType
import org.jooq.Field
import org.jooq.SQLDialect
import org.jooq.conf.ParamType
import org.jooq.impl.DSL
import org.jooq.impl.DefaultDataType
import org.jooq.impl.SQLDataType
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.testcontainers.containers.MySQLContainer

class MysqlSqlGeneratorIntegrationTest : JdbcSqlGeneratorIntegrationTest<MinimumDestinationState>() {
  class MysqlSourceOperations : JdbcSourceOperations() {
    @Throws(SQLException::class)
    override fun copyToJsonField(resultSet: ResultSet, colIndex: Int, json: ObjectNode) {
      val columnName = resultSet.metaData.getColumnName(colIndex)
      val columnTypeName = resultSet.metaData.getColumnTypeName(colIndex).lowercase(Locale.getDefault())

      // JSONB has no equivalent in JDBCType
      if ("json" == columnTypeName) {
        json.set<JsonNode>(columnName, Jsons.deserializeExact(resultSet.getString(colIndex)))
      } else {
        super.copyToJsonField(resultSet, colIndex, json)
      }
    }
  }

  override val sqlGenerator: JdbcSqlGenerator
    get() = MysqlSqlGenerator(MySQLNameTransformer())

  override val destinationHandler: DestinationHandler<MinimumDestinationState>
    get() =// Mysql doesn't have an actual schema concept.
// All of our queries pass a value into the "schemaName" parameter, which mysql treats as being
// the database name.
// So we pass null for the databaseName parameter here, because we don't use the 'test' database at all.
      // TODO should we make this param nullabe on JdbcDestinationHandler?
      MysqlDestinationHandler("test", Companion.database, namespace)

  @Throws(Exception::class)
  override fun insertRawTableRecords(streamId: StreamId, records: List<JsonNode>) {
    reformatMetaColumnTimestamps(records)
    super.insertRawTableRecords(streamId, records)
  }

  @Throws(Exception::class)
  override fun insertFinalTableRecords(
    includeCdcDeletedAt: Boolean,
    streamId: StreamId,
    suffix: String?,
    records: List<JsonNode>
  ) {
    reformatMetaColumnTimestamps(records)
    super.insertFinalTableRecords(includeCdcDeletedAt, streamId, suffix, records)
  }

  @Throws(Exception::class)
  override fun insertV1RawTableRecords(streamId: StreamId, records: List<JsonNode>) {
    reformatMetaColumnTimestamps(records)
    super.insertV1RawTableRecords(streamId, records)
  }

  @Throws(Exception::class)
  override fun createRawTable(streamId: StreamId) {
    database.execute(
      dslContext.createTable(DSL.name(streamId.rawNamespace, streamId.rawName))
        .column(
          COLUMN_NAME_AB_RAW_ID,
          SQLDataType.VARCHAR(36).nullable(false)
        ) // we use VARCHAR for timestamp values, but TIMESTAMP(6) for extracted+loaded_at.
        // because legacy normalization did that. :shrug:
        .column(COLUMN_NAME_AB_EXTRACTED_AT, SQLDataType.TIMESTAMP(6).nullable(false))
        .column(COLUMN_NAME_AB_LOADED_AT, SQLDataType.TIMESTAMP(6))
        .column(COLUMN_NAME_DATA, structType.nullable(false))
        .column(COLUMN_NAME_AB_META, structType.nullable(true))
        .getSQL(ParamType.INLINED)
    )
  }

  @Throws(Exception::class)
  override fun createV1RawTable(v1RawTable: StreamId) {
    database.execute(
      dslContext.createTable(DSL.name(v1RawTable.rawNamespace, v1RawTable.rawName))
        .column(
          COLUMN_NAME_AB_ID,
          SQLDataType.VARCHAR(36).nullable(false)
        ) // similar to createRawTable - this data type is timestmap, not varchar
        .column(COLUMN_NAME_EMITTED_AT, SQLDataType.TIMESTAMP(6).nullable(false))
        .column(COLUMN_NAME_DATA, structType.nullable(false))
        .getSQL(ParamType.INLINED)
    )
  }

  @Test
  @Throws(Exception::class)
  override fun testCreateTableIncremental() {
    // TODO
    val sql = generator.createTable(incrementalDedupStream, "", false)
    destinationHandler.execute(sql)

    val initialStatuses = destinationHandler.gatherInitialState(listOf(incrementalDedupStream))
    Assertions.assertEquals(1, initialStatuses.size)
    val initialStatus = initialStatuses.first()
    Assertions.assertTrue(initialStatus.isFinalTablePresent)
    Assertions.assertFalse(initialStatus.isSchemaMismatch)
  }

  override val database: JdbcDatabase
    get() = Companion.database

  override val structType: DataType<*>
    get() = DefaultDataType(null, String::class.java, "json")

  override val sqlDialect: SQLDialect
    get() = SQLDialect.MYSQL

  override fun toJsonValue(valueAsString: String?): Field<*> {
    // mysql lets you just insert json strings directly into json columns
    return DSL.`val`(valueAsString)
  }

  @Throws(Exception::class)
  override fun teardownNamespace(namespace: String?) {
    // mysql doesn't have a CASCADE keyword in DROP SCHEMA, so we have to override this method.
    // we're currently on jooq 3.13; jooq's dropDatabase() call was only added in 3.14
    database.execute(dslContext.dropSchema(namespace).getSQL(ParamType.INLINED))
  }

  override val supportsSafeCast: Boolean
    get() = false

  companion object {
    private lateinit var testContainer: MySQLContainer<*>
    private lateinit var database: JdbcDatabase

      @JvmStatic
      @BeforeAll
      @Throws(Exception::class)
      fun setupMysql() {
          testContainer = MySQLContainer("mysql:8.0")
          testContainer.start()
          MySQLDestinationAcceptanceTest.configureTestContainer(testContainer)

          val config: JsonNode =
              MySQLDestinationAcceptanceTest.getConfigFromTestContainer(testContainer)

          // TODO move this into JdbcSqlGeneratorIntegrationTest?
          // This code was largely copied from RedshiftSqlGeneratorIntegrationTest
          // TODO: Its sad to instantiate unneeded dependency to construct database and datsources. pull it to
          // static methods.
          val insertDestination: MySQLDestination = MySQLDestination()
          val dataSource: DataSource = insertDestination.getDataSource(config)
          database = DefaultJdbcDatabase(dataSource, MysqlSourceOperations())
      }

      @JvmStatic
      @AfterAll
      fun teardownMysql() {
          testContainer.stop()
          testContainer.close()
      }

    private fun reformatMetaColumnTimestamps(records: List<JsonNode>) {
      // We use mysql's TIMESTAMP(6) type for extracted_at+loaded_at.
      // Unfortunately, mysql doesn't allow you to use the 'Z' suffix for UTC timestamps.
      // Convert those to '+00:00' here.
      for (record in records) {
        reformatTimestampIfPresent(record, COLUMN_NAME_AB_EXTRACTED_AT)
        reformatTimestampIfPresent(record, COLUMN_NAME_EMITTED_AT)
        reformatTimestampIfPresent(record, COLUMN_NAME_AB_LOADED_AT)
      }
    }

    private fun reformatTimestampIfPresent(record: JsonNode, columnNameAbExtractedAt: String) {
      if (record.has(columnNameAbExtractedAt)) {
        val extractedAt = OffsetDateTime.parse(record[columnNameAbExtractedAt].asText())
        val reformattedExtractedAt: String = TIMESTAMP_FORMATTER.format(extractedAt)
        (record as ObjectNode).put(columnNameAbExtractedAt, reformattedExtractedAt)
      }
    }
  }
}
