/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.greptime.flink.source;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GreptimeDynamicTableSourceSecurityIT {

    private static final String SECRET = "review-secret";

    @Test
    void shouldNotExposeSensitiveJdbcUrlThroughFlinkFactoryErrors() throws Exception {
        assertSanitizedFailure(
                "sensitive_source",
                "jdbc:mysql://127.0.0.1:4002/public?trustCertificateKeyStorePassword=" + SECRET,
                ", 'query.fetch-size' = 'invalid'",
                "credentials or authentication tokens");
    }

    @Test
    void shouldNotExposeMalformedJdbcUrlThroughFlinkFactoryErrors() throws Exception {
        assertSanitizedFailure(
                "malformed_source",
                "jdbc:mysql://127.0.0.1:4002/public?pa%zzsword=" + SECRET,
                "",
                "Invalid percent-encoding in `query.jdbc-url`");
    }

    @Test
    void shouldNotExposeAuthorityPropertySecretThroughFlinkFactoryErrors() throws Exception {
        assertSanitizedFailure(
                "authority_property_source",
                "jdbc:mysql://address=(host=127.0.0.1)(port=4002)(password=" + SECRET + ")/public",
                ", 'query.fetch-size' = 'invalid'",
                "credentials or authentication tokens");
    }

    @Test
    void shouldNotExposeEncodedAuthorityUserInfoThroughFlinkFactoryErrors() throws Exception {
        assertSanitizedFailure(
                "encoded_authority_user_info_source",
                "jdbc:mysql://reader:" + SECRET + "%40127.0.0.1:4002/public",
                ", 'query.fetch-size' = 'invalid'",
                "credentials or authentication tokens");
    }

    private static void assertSanitizedFailure(
            String tableName,
            String jdbcUrl,
            String extraOptions,
            String expectedMessage) throws Exception {
        TableEnvironment tableEnvironment = TableEnvironment.create(
                EnvironmentSettings.newInstance().inBatchMode().build());
        tableEnvironment.executeSql("CREATE TEMPORARY TABLE " + tableName + " ("
                + "host STRING"
                + ") WITH ("
                + "'connector' = 'greptimedb', "
                + "'query.jdbc-url' = '" + jdbcUrl + "'"
                + extraOptions
                + ")").await();

        Exception error = assertThrows(Exception.class, () -> {
            try (CloseableIterator<Row> rows = tableEnvironment
                    .executeSql("SELECT host FROM " + tableName)
                    .collect()) {
                rows.hasNext();
            }
        });
        String messages = collectCauseMessages(error);

        assertTrue(messages.contains(expectedMessage), messages);
        assertFalse(messages.contains(SECRET), messages);
    }

    private static String collectCauseMessages(Throwable error) {
        StringBuilder messages = new StringBuilder();
        Throwable current = error;
        while (current != null) {
            messages.append(current).append('\n');
            current = current.getCause();
        }
        return messages.toString();
    }
}
