package io.airbyte.integrations.destination.mysql.typing_deduping

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.cdk.db.jdbc.JdbcDatabase
import io.airbyte.cdk.integrations.destination.jdbc.typing_deduping.JdbcDestinationHandler
import io.airbyte.integrations.base.destination.typing_deduping.AirbyteProtocolType
import io.airbyte.integrations.base.destination.typing_deduping.AirbyteType
import io.airbyte.integrations.base.destination.typing_deduping.Array
import io.airbyte.integrations.base.destination.typing_deduping.Struct
import io.airbyte.integrations.base.destination.typing_deduping.Union
import io.airbyte.integrations.base.destination.typing_deduping.UnsupportedOneOf
import io.airbyte.integrations.base.destination.typing_deduping.migrators.MinimumDestinationState
import java.time.Instant
import org.jooq.SQLDialect
import org.slf4j.Logger
import org.slf4j.LoggerFactory

class MysqlDestinationHandler(
    databaseName: String?,
    jdbcDatabase: JdbcDatabase?,
    rawTableSchema: String?
) :
    JdbcDestinationHandler<MinimumDestinationState>(
        databaseName!!, jdbcDatabase!!, rawTableSchema!!, SQLDialect.MYSQL,
    ) {
    override fun toJdbcTypeName(airbyteType: AirbyteType): String {
        // This is mostly identical to the postgres implementation, but swaps jsonb to super
        if (airbyteType is AirbyteProtocolType) {
            return Companion.toJdbcTypeName(airbyteType)
        }
        return when (airbyteType.typeName) {
            Struct.TYPE, UnsupportedOneOf.TYPE, Array.TYPE -> "json"
            Union.TYPE -> toJdbcTypeName((airbyteType as Union).chooseType())
            else -> throw IllegalArgumentException("Unsupported AirbyteType: $airbyteType")
        }
    }

    override fun toDestinationState(json: JsonNode): MinimumDestinationState {
        return MinimumDestinationState.Impl(
            json.hasNonNull("needsSoftReset") && json["needsSoftReset"].asBoolean(),
        )
    }

    companion object {
        private val LOGGER: Logger = LoggerFactory.getLogger(
            MysqlDestinationHandler::class.java,
        )

        // TODO is this still necessary?
        //  // mysql's ResultSet#getTimestamp() throws errors like
        //  // `java.sql.SQLDataException: Cannot convert string '2023-01-01T00:00:00Z' to java.sql.Timestamp value`
        //  // so we override the method and replace all of those calls with Instant.parse(rs.getString())
        //  // yes, this is dumb.
        //  @Override
        //  public InitialRawTableState getInitialRawTableState(final StreamId id) throws Exception {
        //    final boolean tableExists = jdbcDatabase.executeMetadataQuery(dbmetadata -> {
        //      LOGGER.info("Retrieving table from Db metadata: {} {} {}", databaseName, id.rawNamespace(), id.rawName());
        //      try (final ResultSet table = dbmetadata.getTables(databaseName, id.rawNamespace(), id.rawName(), null)) {
        //        return table.next();
        //      } catch (final SQLException e) {
        //        LOGGER.error("Failed to retrieve table info from metadata", e);
        //        throw new SQLRuntimeException(e);
        //      }
        //    });
        //    if (!tableExists) {
        //      // There's no raw table at all. Therefore there are no unprocessed raw records, and this sync
        //      // should not filter raw records by timestamp.
        //      return new InitialRawTableState(false, Optional.empty());
        //    }
        //    // And use two explicit queries because COALESCE might not short-circuit evaluation.
        //    // This first query tries to find the oldest raw record with loaded_at = NULL.
        //    // Unsafe query requires us to explicitly close the Stream, which is inconvenient,
        //    // but it's also the only method in the JdbcDatabase interface to return non-string/int types
        //    try (final Stream<Instant> timestampStream = jdbcDatabase.unsafeQuery(
        //        conn -> conn.prepareStatement(
        //            getDslContext().select(field("MIN(_airbyte_extracted_at)").as("min_timestamp"))
        //                .from(name(id.rawNamespace(), id.rawName()))
        //                .where(DSL.condition("_airbyte_loaded_at IS NULL"))
        //                .getSQL()),
        //        record -> parseInstant(record.getString("min_timestamp")))) {
        //      // Filter for nonNull values in case the query returned NULL (i.e. no unloaded records).
        //      final Optional<Instant> minUnloadedTimestamp = timestampStream.filter(Objects::nonNull).findFirst();
        //      if (minUnloadedTimestamp.isPresent()) {
        //        // Decrement by 1 second since timestamp precision varies between databases.
        //        final Optional<Instant> ts = minUnloadedTimestamp
        //            .map(i -> i.minus(1, ChronoUnit.SECONDS));
        //        return new InitialRawTableState(true, ts);
        //      }
        //    }
        //    // If there are no unloaded raw records, then we can safely skip all existing raw records.
        //    // This second query just finds the newest raw record.
        //    try (final Stream<Instant> timestampStream = jdbcDatabase.unsafeQuery(
        //        conn -> conn.prepareStatement(
        //            getDslContext().select(field("MAX(_airbyte_extracted_at)").as("min_timestamp"))
        //                .from(name(id.rawNamespace(), id.rawName()))
        //                .getSQL()),
        //        record -> parseInstant(record.getString("min_timestamp")))) {
        //      // Filter for nonNull values in case the query returned NULL (i.e. no raw records at all).
        //      final Optional<Instant> minUnloadedTimestamp = timestampStream.filter(Objects::nonNull).findFirst();
        //      return new InitialRawTableState(false, minUnloadedTimestamp);
        //    }
        //  }
        private fun parseInstant(ts: String?): Instant? {
            // Instant.parse requires nonnull input.
            if (ts == null) {
                return null
            }
            return Instant.parse(ts)
        }

        private fun toJdbcTypeName(airbyteProtocolType: AirbyteProtocolType): String {
            return when (airbyteProtocolType) {
                AirbyteProtocolType.STRING -> "text"
                AirbyteProtocolType.NUMBER -> "decimal"
                AirbyteProtocolType.INTEGER -> "bigint"
                AirbyteProtocolType.BOOLEAN -> "boolean"
                AirbyteProtocolType.TIMESTAMP_WITH_TIMEZONE -> "varchar"
                AirbyteProtocolType.TIMESTAMP_WITHOUT_TIMEZONE -> "datetime"
                AirbyteProtocolType.TIME_WITH_TIMEZONE -> "varchar"
                AirbyteProtocolType.TIME_WITHOUT_TIMEZONE -> "time"
                AirbyteProtocolType.DATE -> "date"
                AirbyteProtocolType.UNKNOWN -> "json"
            }
        }
    }
}
