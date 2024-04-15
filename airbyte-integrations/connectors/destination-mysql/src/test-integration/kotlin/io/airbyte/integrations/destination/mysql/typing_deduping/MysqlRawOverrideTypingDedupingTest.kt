package io.airbyte.integrations.destination.mysql.typing_deduping

import com.fasterxml.jackson.databind.node.ObjectNode

class MysqlRawOverrideTypingDedupingTest : AbstractMysqlTypingDedupingTest() {

    override val baseConfig: ObjectNode =
        super.baseConfig.put("raw_table_database", "overridden_raw_dataset")
}
