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

import java.util.Arrays;
import java.util.Comparator;

import org.apache.metamodel.util.SimpleTableDef;
import org.junit.Test;
import org.neo4j.driver.v1.AuthToken;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.Statement;

public class Neo4jSchemaDetectorTest extends Neo4jTestCase {

	Driver _driver;

	@Override
	protected void setUp() throws Exception {
		super.setUp();

		if (isConfigured()) {
			if ((getUsername() != null) && (getPassword() != null)) {
				AuthToken authToken = AuthTokens.basic(getUsername(), getPassword());
				_driver = GraphDatabase.driver("bolt://" + getHostname() + ":" + getPort(), authToken);
			} else {
				_driver = GraphDatabase.driver("bolt://" + getHostname() + ":" + getPort());
			}
		}
	}
	
	@Test
    public void testDetectSchemaNoRelationship() throws Exception {
        if (!isConfigured()) {
            System.err.println(getInvalidConfigurationMessage());
            return;
        }

        Statement createStatement = new Statement(
                "CREATE (n1:ApacheMetaModelLabel1 { property1: 1, property2: 2 }), (n2:ApacheMetaModelLabel2 { property1: 1, property3: 3 })");
        Session session = _driver.session();
        session.run(createStatement);
        session.close();

        SimpleTableDef[] tableDefs = Neo4jSchemaDetector.detectTableDefs(_driver);
        
        assertEquals(2, tableDefs.length);
        Arrays.sort(tableDefs, new Comparator<SimpleTableDef>() {

            @Override
            public int compare(SimpleTableDef o1, SimpleTableDef o2) {
                return o1.toString().compareTo(o2.toString());
            }
        });
        
        // Table 1
        assertEquals("ApacheMetaModelLabel1", tableDefs[0].getName());
        String[] label1ColumnNames = tableDefs[0].getColumnNames();
        assertEquals(3, label1ColumnNames.length);
        
        Arrays.sort(label1ColumnNames);
        assertEquals("id", label1ColumnNames[0]);
        assertEquals("property1", label1ColumnNames[1]);
        assertEquals("property2", label1ColumnNames[2]);
        
        // Table 2
        String[] label2ColumnNames = tableDefs[1].getColumnNames();
        assertEquals(3, label2ColumnNames.length);
        
        Arrays.sort(label2ColumnNames);
        assertEquals("id", label2ColumnNames[0]);
        assertEquals("property1", label2ColumnNames[1]);
        assertEquals("property3", label2ColumnNames[2]);
    }

	@Test
	public void testDetectSchemaUnidirectionalRelationship() throws Exception {
		if (!isConfigured()) {
			System.err.println(getInvalidConfigurationMessage());
			return;
		}

		Statement createStatement = new Statement(
				"CREATE (n1:ApacheMetaModelLabel1 { property1: 1, property2: 2 })-[r:ApacheMetaModelRelationship { relprop1: 1, relprop2: 2 }]->(n2:ApacheMetaModelLabel2 { property1: 1, property3: 3 })");
		Session session = _driver.session();
		session.run(createStatement);
		session.close();

		SimpleTableDef[] tableDefs = Neo4jSchemaDetector.detectTableDefs(_driver);
		
		assertEquals(2, tableDefs.length);
		Arrays.sort(tableDefs, new Comparator<SimpleTableDef>() {

			@Override
			public int compare(SimpleTableDef o1, SimpleTableDef o2) {
				return o1.toString().compareTo(o2.toString());
			}
		});
		
		// Table 1
		assertEquals("ApacheMetaModelLabel1", tableDefs[0].getName());
		String[] label1ColumnNames = tableDefs[0].getColumnNames();
		assertEquals(6, label1ColumnNames.length);
		
		Arrays.sort(label1ColumnNames);
		assertEquals("id", label1ColumnNames[0]);
		assertEquals("property1", label1ColumnNames[1]);
		assertEquals("property2", label1ColumnNames[2]);
		assertEquals("rel_ApacheMetaModelRelationship", label1ColumnNames[3]);
		assertEquals("rel_ApacheMetaModelRelationship#relprop1", label1ColumnNames[4]);
		assertEquals("rel_ApacheMetaModelRelationship#relprop2", label1ColumnNames[5]);
		
		// Table 2
		String[] label2ColumnNames = tableDefs[1].getColumnNames();
		assertEquals(3, label2ColumnNames.length);
		
		Arrays.sort(label2ColumnNames);
		assertEquals("id", label2ColumnNames[0]);
		assertEquals("property1", label2ColumnNames[1]);
		assertEquals("property3", label2ColumnNames[2]);
	}
	
	@Test
    public void testDetectSchemaBidirectionalRelationship() throws Exception {
        if (!isConfigured()) {
            System.err.println(getInvalidConfigurationMessage());
            return;
        }

        Statement createNodesStatement = new Statement(
                "CREATE (n1:ApacheMetaModelLabel1 { property1: 1, property2: 2 }), (n2:ApacheMetaModelLabel2 { property1: 1, property3: 3 })");
        Statement createRelationshipStatement = new Statement(
                "MATCH (n1:ApacheMetaModelLabel1 { property1: 1, property2: 2 }), (n2:ApacheMetaModelLabel2 { property1: 1, property3: 3 }) CREATE (n1)-[r1:ApacheMetaModelRelationship { relprop1: 1, relprop2: 2 }]->(n2), (n1)<-[r2:ApacheMetaModelRelationship { relprop1: 1, relprop2: 2 }]-(n2)");
        Session session = _driver.session();
        session.run(createNodesStatement);
        session.run(createRelationshipStatement);
        session.close();

        SimpleTableDef[] tableDefs = Neo4jSchemaDetector.detectTableDefs(_driver);
        
        assertEquals(2, tableDefs.length);
        Arrays.sort(tableDefs, new Comparator<SimpleTableDef>() {

            @Override
            public int compare(SimpleTableDef o1, SimpleTableDef o2) {
                return o1.toString().compareTo(o2.toString());
            }
        });
        
        // Table 1
        assertEquals("ApacheMetaModelLabel1", tableDefs[0].getName());
        String[] label1ColumnNames = tableDefs[0].getColumnNames();
        assertEquals(6, label1ColumnNames.length);
        
        Arrays.sort(label1ColumnNames);
        assertEquals("id", label1ColumnNames[0]);
        assertEquals("property1", label1ColumnNames[1]);
        assertEquals("property2", label1ColumnNames[2]);
        assertEquals("rel_ApacheMetaModelRelationship", label1ColumnNames[3]);
        assertEquals("rel_ApacheMetaModelRelationship#relprop1", label1ColumnNames[4]);
        assertEquals("rel_ApacheMetaModelRelationship#relprop2", label1ColumnNames[5]);
        
        // Table 2
        assertEquals("ApacheMetaModelLabel2", tableDefs[1].getName());
        String[] label2ColumnNames = tableDefs[1].getColumnNames();
        assertEquals(6, label2ColumnNames.length);
        
        Arrays.sort(label2ColumnNames);
        assertEquals("id", label2ColumnNames[0]);
        assertEquals("property1", label2ColumnNames[1]);
        assertEquals("property3", label2ColumnNames[2]);
        assertEquals("rel_ApacheMetaModelRelationship", label2ColumnNames[3]);
        assertEquals("rel_ApacheMetaModelRelationship#relprop1", label2ColumnNames[4]);
        assertEquals("rel_ApacheMetaModelRelationship#relprop2", label2ColumnNames[5]);
    }

	@Override
	protected void tearDown() throws Exception {
		if (isConfigured()) {
			// Delete the test nodes
			Session session = _driver.session();
			session.run("MATCH (n) OPTIONAL MATCH (n)-[r]-() WHERE n:ApacheMetaModelLabel OR n:ApacheMetaModelLabel1 OR n:ApacheMetaModelLabel2 DELETE n, r;");
			session.close();
			_driver.close();
		}

		super.tearDown();
	}

}
