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

import org.apache.metamodel.util.SimpleTableDef;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.Statement;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.util.Function;

final class Neo4jSchemaDetector {

    public static SimpleTableDef[] detectTableDefs(Driver driver) {
        List<SimpleTableDef> tableDefs = new ArrayList<SimpleTableDef>();

        try (Session session = driver.session()) {
            List<String> labels = detectLabels(session);
            
            for (String label : labels) {
                List<String> columns = new ArrayList<>();
                
                List<String> propertiesPerLabel = detectPropertiesPerLabel(session, label);
                columns.addAll(propertiesPerLabel);
                
                List<String> relationshipTypesPerLabel = detectRelationshipTypesPerLabel(session, label);
                columns.addAll(relationshipTypesPerLabel);
               
                for (final String relationshipType : relationshipTypesPerLabel) {
                    List<String> relationshipPropertiesPerType = detectRelationshipPropertiesPerType(session, label,
                            relationshipType);
                    columns.addAll(relationshipPropertiesPerType);
                }
                
                SimpleTableDef tableDef = new SimpleTableDef(label,
                        columns.toArray(new String[columns.size()]));
                tableDefs.add(tableDef);
            }
        }

        return tableDefs.toArray(new SimpleTableDef[tableDefs.size()]);
    }

    private static List<String> detectRelationshipPropertiesPerType(Session session, String label,
            final String relationshipType) {
        String relationshipTypeNoPrefix = relationshipType.substring(4); // Strip "rel_" prefix for lookup
        
        String statementString = "MATCH (:" + label + ")-[rel:" + relationshipTypeNoPrefix + "]->() "
                + "WITH DISTINCT keys(rel) as relationshipProperties UNWIND relationshipProperties as relationshipProperty "
                + "RETURN DISTINCT relationshipProperty";
        
        Statement getAllRelationshipPropertiesPerTypeStatement = new Statement(statementString );
        StatementResult relationshipPropertiesPerTypeResult = session.run(getAllRelationshipPropertiesPerTypeStatement);
        
        List<String> relationshipPropertiesPerType = relationshipPropertiesPerTypeResult.list(new Function<Record, String>() {

            @Override
            public String apply(Record record) {
                return relationshipType + "#" + record.get(0).asString();
            }
            
        });
        
        return relationshipPropertiesPerType;
    }

    private static List<String> detectRelationshipTypesPerLabel(Session session, String label) {
        String statementString = "MATCH (:" + label + ")-[rel]->() "
                + "RETURN DISTINCT type(rel) as relationshipType ORDER BY relationshipType";
        
        Statement getAllRelationshipTypesPerLabelStatement = new Statement(statementString);
        StatementResult relationshipTypesPerLabelResult = session.run(getAllRelationshipTypesPerLabelStatement);
        
        List<String> relationshipTypesPerLabel = relationshipTypesPerLabelResult.list(new Function<Record, String>() {

            @Override
            public String apply(Record record) {
                return "rel_" + record.get(0).asString();
            }
            
        });
        
        return relationshipTypesPerLabel;
    }

    private static List<String> detectPropertiesPerLabel(Session session, String label) {
        String statementString = "MATCH (n:" + label + ") WITH keys(n) as properties "
                + "UNWIND properties as property RETURN distinct property ORDER BY property";
        
        Statement getAllPropertiesPerLabelStatement = new Statement(statementString );
        StatementResult propertiesPerLabelResult = session.run(getAllPropertiesPerLabelStatement);
        
        List<String> propertiesPerLabel = propertiesPerLabelResult.list(new Function<Record, String>() {

            @Override
            public String apply(Record record) {
                return record.get(0).asString();
            }
            
        });
        
        propertiesPerLabel.add(0, "id");
        
        return propertiesPerLabel;
    }
    
    private static List<String> detectLabels(Session session) {
        String statementString = "MATCH (n) WITH DISTINCT labels(n) as labels "
                + "UNWIND labels as label RETURN distinct label ORDER BY label";
        
        Statement getAllLabelsStatement = new Statement(statementString);
        
        StatementResult allLabelsResult = session.run(getAllLabelsStatement);
        
        return allLabelsResult.list(new Function<Record, String>() {

            @Override
            public String apply(Record record) {
                return record.get(0).asString();
            }
            
        });
    }

}
