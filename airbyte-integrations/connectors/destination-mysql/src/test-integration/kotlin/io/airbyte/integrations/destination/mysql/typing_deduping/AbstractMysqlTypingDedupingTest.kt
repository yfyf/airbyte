package io.airbyte.integrations.destination.mysql.typing_deduping;

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode
import io.airbyte.cdk.integrations.destination.NamingConventionTransformer
import io.airbyte.cdk.integrations.standardtest.destination.typing_deduping.JdbcTypingDedupingTest
import io.airbyte.commons.text.Names
import io.airbyte.integrations.base.destination.typing_deduping.SqlGenerator
import io.airbyte.integrations.base.destination.typing_deduping.StreamId.Companion.concatenateRawTableName
import io.airbyte.integrations.destination.mysql.MySQLDestination
import io.airbyte.integrations.destination.mysql.MySQLDestinationAcceptanceTest
import io.airbyte.integrations.destination.mysql.MySQLNameTransformer
import io.airbyte.integrations.destination.mysql.MysqlTestSourceOperations
import javax.sql.DataSource
import org.jooq.SQLDialect
import org.jooq.conf.ParamType
import org.jooq.impl.DSL.name
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.testcontainers.containers.MySQLContainer

abstract class AbstractMysqlTypingDedupingTest(
    override val imageName: String = "airbyte/destination-mysql:dev",
    override val dialect: SQLDialect = SQLDialect.MYSQL,
    override val sqlGenerator: SqlGenerator = MysqlSqlGenerator(),
    override val sourceOperations: MysqlTestSourceOperations = MysqlTestSourceOperations(),
    override val nameTransformer: NamingConventionTransformer = MySQLNameTransformer(),
    override val baseConfig: ObjectNode = Companion.config,
) : JdbcTypingDedupingTest() {

    override fun getDataSource(config: JsonNode?): DataSource =
        MySQLDestination().getDataSource(baseConfig)

    override fun disableFinalTableComparison(): Boolean {
        // TODO delete this in the next stacked PR
        return true
    }

    @Throws(Exception::class)
    override fun dumpRawTableRecords(streamNamespace: String?, streamName: String): List<JsonNode> {
        var streamNamespace = streamNamespace
        if (streamNamespace == null) {
            streamNamespace = getDefaultSchema(config!!)
        }
        // Wrap in getIdentifier as a hack for weird mysql name transformer behavior
        val tableName = nameTransformer.getIdentifier(
            nameTransformer.convertStreamName(
                concatenateRawTableName(
                    streamNamespace,
                    Names.toAlphanumericAndUnderscore(streamName),
                ),
            )
        )
        val schema = rawSchema
        return database!!.queryJsons(dslContext.selectFrom(name(schema, tableName)).sql)
    }

    @Throws(Exception::class)
    override fun teardownStreamAndNamespace(streamNamespace: String?, streamName: String) {
        var streamNamespace = streamNamespace
        if (streamNamespace == null) {
            streamNamespace = getDefaultSchema(config!!)
        }
        database!!.execute(
            dslContext.dropTableIfExists(
                name(
                    rawSchema,
                    // Wrap in getIdentifier as a hack for weird mysql name transformer behavior
                    nameTransformer.getIdentifier(
                        concatenateRawTableName(
                            streamNamespace,
                            streamName,
                        ),
                    ),
                ),
            ).sql,
        )

        // mysql doesn't have schemas, it only has databases.
        // so override this method to use dropDatabase.
        database!!.execute(dslContext.dropDatabaseIfExists(streamNamespace).getSQL(ParamType.INLINED))
    }

    companion object {
        private lateinit var testContainer: MySQLContainer<*>
        private lateinit var config: ObjectNode

        @JvmStatic
        @BeforeAll
        @Throws(Exception::class)
        fun setupMysql() {
            testContainer = MySQLContainer("mysql:8.0")
            testContainer.start()
            MySQLDestinationAcceptanceTest.configureTestContainer(testContainer)

            config = MySQLDestinationAcceptanceTest.getConfigFromTestContainer(testContainer)
        }

        @JvmStatic
        @AfterAll
        fun teardownMysql() {
            testContainer.stop()
            testContainer.close()
        }
    }
}
