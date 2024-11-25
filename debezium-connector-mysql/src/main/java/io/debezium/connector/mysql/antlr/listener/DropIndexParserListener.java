/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.mysql.antlr.listener;

import io.debezium.connector.mysql.antlr.MySqlAntlrDdlParser;
import io.debezium.ddl.parser.mysql.generated.MySqlParser;
import io.debezium.ddl.parser.mysql.generated.MySqlParserBaseListener;
import io.debezium.relational.TableEditor;
import io.debezium.relational.TableId;

public class DropIndexParserListener extends MySqlParserBaseListener {

    private final MySqlAntlrDdlParser parser;

    public DropIndexParserListener(MySqlAntlrDdlParser parser) {
        this.parser = parser;
    }

    @Override
    public void enterDropIndex(MySqlParser.DropIndexContext ctx) {
        TableId tableId = parser.parseQualifiedTableId(ctx.tableName().fullId());
        TableEditor tableEditor = parser.databaseTables().editTable(tableId);

        parser.runIfNotNull(() -> {
            String indexName = parser.parseName(ctx.uid());
            tableEditor.uniqueKeyColumnNames().remove(indexName);
            parser.databaseTables().overwriteTable(tableEditor.create());
            parser.signalDropIndex(indexName, tableId, ctx);
        }, tableEditor);

        super.enterDropIndex(ctx);
    }
}
