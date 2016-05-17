/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.metamodel.neo4j.v3;

import java.util.ArrayList;
import java.util.List;

import org.apache.metamodel.DataContext;
import org.apache.metamodel.MetaModelException;
import org.apache.metamodel.MetaModelHelper;
import org.apache.metamodel.QueryPostprocessDataContext;
import org.apache.metamodel.data.DataSet;
import org.apache.metamodel.data.DocumentSource;
import org.apache.metamodel.query.FilterItem;
import org.apache.metamodel.query.SelectItem;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.Schema;
import org.apache.metamodel.schema.Table;
import org.apache.metamodel.schema.builder.DocumentSourceProvider;
import org.apache.metamodel.schema.builder.SchemaBuilder;
import org.apache.metamodel.util.SimpleTableDef;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Statement;
import org.neo4j.driver.v1.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;

/**
 * DataContext implementation for Neo4j (version >= 3.0).
 */
public class Neo4jDataContext extends QueryPostprocessDataContext implements DataContext, DocumentSourceProvider {

    public static final Logger logger = LoggerFactory.getLogger(Neo4jDataContext.class);

    public static final String SCHEMA_NAME = "neo4j";

    public static final int DEFAULT_PORT = 7687;
    
    private final SchemaBuilder _schemaBuilder;

    private final Driver _driver;

    public Neo4jDataContext(String hostname, int port, String username, String password, SimpleTableDef... tableDefs) {
        _driver = buildDriver(hostname, port, username, password);
        _schemaBuilder = new Neo4jTableDefSchemaBuilder(tableDefs);
    }

    public Neo4jDataContext(String hostname, int port, String username, String password) {
        _driver = buildDriver(hostname, port, username, password);
        _schemaBuilder = new Neo4jTableDefSchemaBuilder(Neo4jSchemaDetector.detectTableDefs(_driver));
    }

    private Driver buildDriver(String hostname, int port, String username, String password) {
        String connectionString = "bolt://" + hostname + ":" + port;
        if ((username != null) && (password != null)) {
            return GraphDatabase.driver(connectionString, AuthTokens.basic(username, password));
        } else {
            return GraphDatabase.driver(connectionString);
        }
    }

    @Override
    protected String getDefaultSchemaName() throws MetaModelException {
        return _schemaBuilder.getSchemaName();
    }

    @Override
    protected Schema getMainSchema() throws MetaModelException {
        return _schemaBuilder.build();
    }

    @Override
    protected String getMainSchemaName() throws MetaModelException {
        return _schemaBuilder.getSchemaName();
    }

    @Override
    protected DataSet materializeMainSchemaTable(Table table, Column[] columns, int firstRow, int maxRows) {
        if ((columns != null) && (columns.length > 0)) {
        	final SelectItem[] selectItems = MetaModelHelper.createSelectItems(columns);
        	
        	String returnClause = prepareReturnClause(selectItems);
        	
        	String statementString = "MATCH (node:" + table.getName() + ")-[relationship*0..1]->() RETURN " + returnClause + " SKIP {firstRow}";
        	
        	Statement statement;
            if (maxRows > 0) {
                statement = new Statement(statementString + " LIMIT {maxRows};",
                        Values.parameters("firstRow", firstRow - 1, "maxRows", maxRows));
            } else {
                statement = new Statement(statementString,
                        Values.parameters("firstRow", firstRow - 1));
            }
            
            return new Neo4jDataSet(_driver, selectItems, statement);
        } else {
            logger.error("Encountered null or empty columns array for materializing main schema table.");
            throw new IllegalArgumentException("Columns cannot be null or empty array");
        }
    }

    @Override
    protected DataSet materializeMainSchemaTable(Table table, Column[] columns, int maxRows) {
        return materializeMainSchemaTable(table, columns, 1, maxRows);
    }

    @Override
    protected Number executeCountQuery(Table table, List<FilterItem> whereItems, boolean functionApproximationAllowed) {
        return null;
    }

    @Override
    public DocumentSource getMixedDocumentSourceForSampling() {
        return null;
    }

    @Override
    public DocumentSource getDocumentSourceForTable(String sourceCollectionName) {
        return null;
    }
    
    private String prepareReturnClause(final SelectItem[] selectItems) {
        List<String> columnNames = new ArrayList<>();
        
        for (SelectItem selectItem : selectItems) {
            String columnName = selectItem.getColumn().getName();
            
            String prefix;
            if (columnName.startsWith("rel_")) {
                prefix = "relationship.";
                
                if (columnName.contains("#")) {
                    columnName = columnName.substring(columnName.indexOf("#") + 1);
                }
            } else {
                prefix = "node.";
            }
            columnNames.add(prefix + columnName);
        }
        
        return Joiner.on(", ").join(columnNames);
    }
}

