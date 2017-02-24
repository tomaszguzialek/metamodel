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

import org.apache.metamodel.DataContext;
import org.apache.metamodel.data.DataSet;
import org.apache.metamodel.data.FilteredDataSet;
import org.apache.metamodel.data.Row;
import org.apache.metamodel.query.CompiledQuery;
import org.junit.Test;
import org.neo4j.driver.v1.AuthToken;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.Statement;
import org.neo4j.driver.v1.Values;

public class Neo4jDataContextTest extends Neo4jTestCase {

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
	public void testSelectQueryWithProjection() throws Exception {
		if (!isConfigured()) {
			System.err.println(getInvalidConfigurationMessage());
			return;
		}

		Statement createStatement = new Statement("CREATE (n:ApacheMetaModelLabel { property1: 1, property2: 2 })");
		Session session = _driver.session();
		session.run(createStatement);
		session.close();

		Neo4jDataContext dataContext = new Neo4jDataContext(getHostname(), getPort(), getUsername(), getPassword());

		{
			CompiledQuery query = dataContext.query().from("ApacheMetaModelLabel").select("property1").compile();
			try (final DataSet dataSet = dataContext.executeQuery(query)) {
				assertTrue(dataSet.next());
				assertEquals("Row[values=[1]]", dataSet.getRow().toString());
				assertFalse(dataSet.next());
			}
		}
		{
			CompiledQuery query = dataContext.query().from("ApacheMetaModelLabel").select("property1")
					.select("property2").compile();
			try (final DataSet dataSet = dataContext.executeQuery(query)) {
				assertTrue(dataSet.next());
				assertEquals("Row[values=[1, 2]]", dataSet.getRow().toString());
				assertFalse(dataSet.next());
			}
		}
	}
	
	@Test
    public void testSelectQueryWithRelationship() throws Exception {
        if (!isConfigured()) {
            System.err.println(getInvalidConfigurationMessage());
            return;
        }

        Statement createNodesStatement = new Statement(
                "CREATE (n1:ApacheMetaModelLabel1 { property1: \"label1Prop1Val\", property2: \"label1Prop2Val\" }), (n2:ApacheMetaModelLabel2 { property1: \"label2Prop1Val\", property3: \"label2Prop3Val\" })");
        Statement createRelationshipStatement = new Statement(
                "MATCH (n1:ApacheMetaModelLabel1 { property1: \"label1Prop1Val\", property2: \"label1Prop2Val\" }), (n2:ApacheMetaModelLabel2 { property1: \"label2Prop1Val\", property3: \"label2Prop3Val\" }) CREATE (n1)-[r1:ApacheMetaModelRelationship { relprop1: 1, relprop2: 2 }]->(n2)");
        Session session = _driver.session();
        session.run(createNodesStatement);
        session.run(createRelationshipStatement);
        session.close();

        Neo4jDataContext dataContext = new Neo4jDataContext(getHostname(), getPort(), getUsername(), getPassword());

        {
            CompiledQuery query = dataContext.query().from("ApacheMetaModelLabel1").selectAll().compile();
            try (final DataSet dataSet = dataContext.executeQuery(query)) {
                assertTrue(dataSet.next());
                Row row = dataSet.getRow();
                assertEquals(Long.class, row.getValue(0).getClass());
                assertEquals("label1Prop1Val", row.getValue(1));
                assertEquals("label1Prop2Val", row.getValue(2));
                assertEquals(Long.class, row.getValue(3).getClass());
                assertEquals(1L, row.getValue(4));
                assertEquals(2L, row.getValue(5));
                assertFalse(dataSet.next());
            }
        }
    }
	
	@Test
    public void testSelectQueryWithSelfRelationshipUnidirectional() throws Exception {
        if (!isConfigured()) {
            System.err.println(getInvalidConfigurationMessage());
            return;
        }

        Statement createNodesStatement = new Statement(
                "CREATE (n1:ApacheMetaModelLabel { property1: \"label1Prop1Val\", property2: \"label1Prop2Val\" }), (n2:ApacheMetaModelLabel { property1: \"label2Prop1Val\", property3: \"label2Prop3Val\" })");
        Statement createRelationshipStatement = new Statement(
                "MATCH (n1:ApacheMetaModelLabel { property1: \"label1Prop1Val\", property2: \"label1Prop2Val\" }), (n2:ApacheMetaModelLabel { property1: \"label2Prop1Val\", property3: \"label2Prop3Val\" }) CREATE (n1)-[r1:ApacheMetaModelRelationship { relprop1: 1, relprop2: 2 }]->(n2)");
        Session session = _driver.session();
        session.run(createNodesStatement);
        session.run(createRelationshipStatement);
        session.close();

        Neo4jDataContext dataContext = new Neo4jDataContext(getHostname(), getPort(), getUsername(), getPassword());

        {
            CompiledQuery query = dataContext.query().from("ApacheMetaModelLabel").selectAll().compile();
            try (final DataSet dataSet = dataContext.executeQuery(query)) {
                assertTrue(dataSet.next());
                Row row = dataSet.getRow();
                assertEquals(Long.class, row.getValue(0).getClass());
                assertEquals("label1Prop1Val", row.getValue(1));
                assertEquals("label1Prop2Val", row.getValue(2));
                assertEquals(null, row.getValue(3));
                assertEquals(Long.class, row.getValue(4).getClass());
                assertEquals(1L, row.getValue(5));
                assertEquals(2L, row.getValue(6));
                assertTrue(dataSet.next());
                
                row = dataSet.getRow();
                assertEquals(Long.class, row.getValue(0).getClass());
                assertEquals("label2Prop1Val", row.getValue(1));
                assertEquals(null, row.getValue(2));
                assertEquals("label2Prop3Val", row.getValue(3));
                assertEquals(null, row.getValue(4));
                assertEquals(null, row.getValue(5));
                assertEquals(null, row.getValue(6));
                assertFalse(dataSet.next());
            }
        }
    }
	
	@Test
    public void testSelectQueryWithSelfRelationshipBidirectional() throws Exception {
        if (!isConfigured()) {
            System.err.println(getInvalidConfigurationMessage());
            return;
        }

        Statement createNodesStatement = new Statement(
                "CREATE (n1:ApacheMetaModelLabel { property1: \"label1Prop1Val\", property2: \"label1Prop2Val\" }), (n2:ApacheMetaModelLabel { property1: \"label2Prop1Val\", property3: \"label2Prop3Val\" })");
        Statement createRelationshipStatement = new Statement(
                "MATCH (n1:ApacheMetaModelLabel { property1: \"label1Prop1Val\", property2: \"label1Prop2Val\" }), (n2:ApacheMetaModelLabel { property1: \"label2Prop1Val\", property3: \"label2Prop3Val\" }) CREATE (n1)-[r1:ApacheMetaModelRelationship { relprop1: 1, relprop2: 2 }]->(n2), (n1)<-[r2:ApacheMetaModelRelationship { relprop1: 1, relprop2: 2 }]-(n2)");
        Session session = _driver.session();
        session.run(createNodesStatement);
        session.run(createRelationshipStatement);
        session.close();

        Neo4jDataContext dataContext = new Neo4jDataContext(getHostname(), getPort(), getUsername(), getPassword());

        {
            CompiledQuery query = dataContext.query().from("ApacheMetaModelLabel").selectAll().compile();
            try (final DataSet dataSet = dataContext.executeQuery(query)) {
                assertTrue(dataSet.next());
                Row row = dataSet.getRow();
                Object id1 = row.getValue(0);
                assertEquals(Long.class, id1.getClass());
                assertEquals("label1Prop1Val", row.getValue(1));
                assertEquals("label1Prop2Val", row.getValue(2));
                assertEquals(null, row.getValue(3));
                Object joinedNodeId1 = row.getValue(4);
                assertEquals(Long.class, joinedNodeId1.getClass());
                assertEquals(1L, row.getValue(5));
                assertEquals(2L, row.getValue(6));
                assertTrue(dataSet.next());
                
                row = dataSet.getRow();
                Object id2 = row.getValue(0);
                assertEquals(Long.class, id2.getClass());
                assertEquals("label2Prop1Val", row.getValue(1));
                assertEquals(null, row.getValue(2));
                assertEquals("label2Prop3Val", row.getValue(3));
                Object joinedNodeId2 = row.getValue(4);
                assertEquals(Long.class, joinedNodeId2.getClass());
                assertEquals(1L, row.getValue(5));
                assertEquals(2L, row.getValue(6));
                assertFalse(dataSet.next());
                
                assertEquals(id1, joinedNodeId2);
                assertEquals(id2, joinedNodeId1);
            }
        }
    }

	public void ignoredTestSelectQueryWithLargeDataset() throws Exception {
		if (!isConfigured()) {
			System.err.println(getInvalidConfigurationMessage());
			return;
		}

		int rowCount = 100000;

		Session session = _driver.session();
		for (int j = 0; j < rowCount / 10000; j++) {
			for (int i = 0; i < 10000; i++) {
				Statement statement = new Statement("CREATE (n:ApacheMetaModelLabel { i: {iParam}})",
						Values.parameters("iParam", (j * 10000 + i)));
				session.run(statement);
			}
		}
		session.close();
		System.out.println("Inserted " + rowCount + " rows to the database.");

		Neo4jDataContext dataContext = new Neo4jDataContext(getHostname(), getPort(), getUsername(), getPassword());

		{
			CompiledQuery query = dataContext.query().from("ApacheMetaModelLabel").select("i").orderBy("i").compile();
			try (final DataSet dataSet = dataContext.executeQuery(query)) {
				for (int i = 0; i < rowCount; i++) {
					assertTrue(dataSet.next());
				}
				assertFalse(dataSet.next());
			}
		}
	}

	@Test
	public void testFirstRowAndLastRow() throws Exception {
		if (!isConfigured()) {
			System.err.println(getInvalidConfigurationMessage());
			return;
		}

		// insert a few records
		{
			Session session = _driver.session();

			Statement statement1 = new Statement(
					"CREATE (n:ApacheMetaModelLabel { name: {nameParam}, age: {ageParam} })",
					Values.parameters("nameParam", "John Doe", "ageParam", 30));
			session.run(statement1);

			Statement statement2 = new Statement(
					"CREATE (n:ApacheMetaModelLabel { name: {nameParam}, gender: {genderParam} })",
					Values.parameters("nameParam", "Jane Doe", "genderParam", "F"));
			session.run(statement2);

			session.close();
		}

		// create datacontext using detected schema
		final DataContext dc = new Neo4jDataContext(getHostname(), getPort(), getUsername(), getPassword());

		try (final DataSet ds = dc.query().from("ApacheMetaModelLabel").select("name").and("age").firstRow(2)
				.execute()) {
			assertTrue("Class: " + ds.getClass().getName(), ds instanceof Neo4jDataSet);
			assertTrue(ds.next());
			final Row row = ds.getRow();
			assertEquals(2, row.getValues().length);
			assertEquals("Row[values=[Jane Doe, null]]", row.toString());
			assertFalse(ds.next());
		}

		try (final DataSet ds = dc.query().from("ApacheMetaModelLabel").select("name").and("age").maxRows(1)
				.execute()) {
			assertTrue("Class: " + ds.getClass().getName(), ds instanceof Neo4jDataSet);
			assertTrue(ds.next());
			final Row row = ds.getRow();
			assertEquals("Row[values=[John Doe, 30]]", row.toString());
			assertFalse(ds.next());
		}
	}
	
	@Test
	public void testWhereClause() {
	    if (!isConfigured()) {
            System.err.println(getInvalidConfigurationMessage());
            return;
        }

        // insert a few records
        {
            Session session = _driver.session();

            Statement statement1 = new Statement(
                    "CREATE (n:ApacheMetaModelLabel { name: {nameParam}, age: {ageParam} })",
                    Values.parameters("nameParam", "John Doe", "ageParam", 30));
            session.run(statement1);

            Statement statement2 = new Statement(
                    "CREATE (n:ApacheMetaModelLabel { name: {nameParam}, gender: {genderParam} })",
                    Values.parameters("nameParam", "Jane Doe", "genderParam", "F"));
            session.run(statement2);

            session.close();
        }

        // create datacontext using detected schema
        final DataContext dc = new Neo4jDataContext(getHostname(), getPort(), getUsername(), getPassword());

        try (final DataSet ds = dc.query().from("ApacheMetaModelLabel").selectAll().where("name").eq("John Doe")
                .execute()) {
            assertTrue("Class: " + ds.getClass().getName(), ds instanceof FilteredDataSet);
            assertTrue(ds.next());
            final Row row = ds.getRow();
            assertEquals(4, row.getValues().length);
            assertEquals(Long.class, row.getValue(0).getClass());
            assertEquals(30L, row.getValue(1));
            assertEquals(null, row.getValue(2));
            assertEquals("John Doe", row.getValue(3));
            assertFalse(ds.next());
        }
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
