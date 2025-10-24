/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.m3ql.parser;

import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.tsdb.lang.m3.m3ql.parser.generated.M3QLParser;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.RootNode;

public class M3ExpressionPrinterTests extends OpenSearchTestCase {

    public void testRoundTrip() {
        String[] queries = {
            "fetch city_id:1 | transformNull | moving 1m sum | sum region | avg",
            "fetch city_name:\"San Francisco\" host:{host1,host2} | sum merchantID | transformNull | moving 1m sum",
            "a = fetch city_id:1 | transformNull; b = fetch city_id:2 | transformNull; a | asPercent(b)" };

        for (String query : queries) {
            try {
                RootNode astRoot = M3QLParser.parse(query, false);
                M3QLExpressionPrinter printer = new M3QLExpressionPrinter();
                String outputExpression = astRoot.accept(printer);
                String expectedQuery = query.replaceAll("\\s+", " ").trim();
                assertEquals("Output m3ql expression does not match", expectedQuery, outputExpression);
            } catch (Exception e) {
                fail("Failed to parse and round trip query: " + query + " with error: " + e.getMessage());
            }
        }
    }
}
