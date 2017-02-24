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

import java.util.List;
import java.util.stream.Collectors;

import org.apache.metamodel.data.AbstractDataSet;
import org.apache.metamodel.data.DefaultRow;
import org.apache.metamodel.data.Row;
import org.apache.metamodel.query.SelectItem;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.Statement;
import org.neo4j.driver.v1.StatementResult;

final class Neo4jDataSet extends AbstractDataSet {

    private Session _session;

    private StatementResult _result;

    private Row _row;

    public Neo4jDataSet(Driver driver, SelectItem[] selectItems, Statement statement) {
        super(selectItems);
        _session = driver.session();
        _result = _session.run(statement);
    }

    @Override
    public boolean next() {
        boolean hasNext = _result.hasNext();
        if (hasNext) {
            Record record = _result.next();

            List<Object> values = record.values().stream().map(x -> x.asObject())
                    .collect(Collectors.toList());

            _row = new DefaultRow(getHeader(), values.toArray());

            return true;
        } else {
            _result.consume();
            return false;
        }
    }

    @Override
    public Row getRow() {
        return _row;
    }

    @Override
    public void close() {
        _session.close();
    }

}
