package com.databricks.jdbc.dbclient.impl.sqlexec;

import static com.databricks.jdbc.TestConstants.TEST_STRING;
import static com.databricks.jdbc.common.DatabricksJdbcConstants.TEMPORARY_REDIRECT_STATUS_CODE;
import static com.databricks.jdbc.dbclient.impl.sqlexec.PathConstants.*;
import static com.databricks.jdbc.model.core.ColumnInfoTypeName.DECIMAL;
import static com.databricks.jdbc.model.core.ColumnInfoTypeName.INT;
import static com.databricks.jdbc.model.core.ColumnInfoTypeName.STRING;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.databricks.jdbc.api.impl.*;
import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.common.IDatabricksComputeResource;
import com.databricks.jdbc.common.StatementType;
import com.databricks.jdbc.common.Warehouse;
import com.databricks.jdbc.common.util.DatabricksTypeUtil;
import com.databricks.jdbc.dbclient.impl.common.ConfiguratorUtilsTest;
import com.databricks.jdbc.dbclient.impl.common.StatementId;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.exception.DatabricksTemporaryRedirectException;
import com.databricks.jdbc.exception.DatabricksTimeoutException;
import com.databricks.jdbc.model.client.sqlexec.*;
import com.databricks.jdbc.model.client.sqlexec.ExecuteStatementRequest;
import com.databricks.jdbc.model.client.sqlexec.ExecuteStatementResponse;
import com.databricks.jdbc.model.core.Disposition;
import com.databricks.jdbc.model.core.ResultData;
import com.databricks.jdbc.model.core.ResultManifest;
import com.databricks.jdbc.model.core.ResultSchema;
import com.databricks.jdbc.model.core.StatementStatus;
import com.databricks.sdk.core.ApiClient;
import com.databricks.sdk.core.DatabricksError;
import com.databricks.sdk.core.http.Request;
import com.databricks.sdk.service.sql.*;
import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.*;
import javax.net.ssl.SSLHandshakeException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class DatabricksSdkClientTest {
  @Mock StatementExecutionService statementExecutionService;
  @Mock ApiClient apiClient;
  @Mock ResultData resultData;
  private static final String WAREHOUSE_ID = "99999999";
  private static final IDatabricksComputeResource warehouse = new Warehouse(WAREHOUSE_ID);
  private static final String SESSION_ID = "session_id";
  private static final StatementId STATEMENT_ID = new StatementId("statementId");
  private static final String STATEMENT =
      "SELECT * FROM orders WHERE user_id = ? AND shard = ? AND region_code = ? AND namespace = ?";
  private static final String JDBC_URL =
      "jdbc:databricks://sample-host.18.azuredatabricks.net:4423/default;transportMode=http;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/99999999;";
  private static final String DEFAULT_KEYSTORE_PASSWORD = "changeit";

  private static final Map<Integer, ImmutableSqlParameter> sqlParams =
      new HashMap<>() {
        {
          put(1, getSqlParam(1, 100, DatabricksTypeUtil.BIGINT));
          put(2, getSqlParam(2, (short) 10, DatabricksTypeUtil.SMALLINT));
          put(3, getSqlParam(3, (byte) 15, DatabricksTypeUtil.TINYINT));
          put(4, getSqlParam(4, "value", DatabricksTypeUtil.STRING));
        }
      };

  private void setupSessionMocks() throws IOException {
    CreateSessionResponse response = new CreateSessionResponse().setSessionId(SESSION_ID);
    when(apiClient.execute(any(Request.class), eq(CreateSessionResponse.class)))
        .thenReturn(response);
  }

  private void setupClientMocks(boolean includeResults, boolean async) throws IOException {
    List<StatementParameterListItem> params =
        new ArrayList<>() {
          {
            add(getParam("LONG", "100", 1));
            add(getParam("SHORT", "10", 2));
            add(getParam("SHORT", "15", 3));
            add(getParam("STRING", "value", 4));
          }
        };

    StatementStatus statementStatus = new StatementStatus().setState(StatementState.SUCCEEDED);
    ExecuteStatementRequest executeStatementRequest =
        new ExecuteStatementRequest()
            .setSessionId(SESSION_ID)
            .setWarehouseId(WAREHOUSE_ID)
            .setStatement(STATEMENT)
            .setDisposition(Disposition.INLINE_OR_EXTERNAL_LINKS)
            .setFormat(Format.ARROW_STREAM)
            .setRowLimit(100L)
            .setParameters(params);
    if (async) {
      executeStatementRequest.setWaitTimeout("0s");
    } else {
      executeStatementRequest
          .setWaitTimeout("10s")
          .setOnWaitTimeout(ExecuteStatementRequestOnWaitTimeout.CONTINUE);
    }
    ExecuteStatementResponse response =
        new ExecuteStatementResponse()
            .setStatementId(STATEMENT_ID.toSQLExecStatementId())
            .setStatus(statementStatus);
    if (includeResults) {
      response
          .setResult(resultData)
          .setManifest(
              new ResultManifest()
                  .setFormat(Format.JSON_ARRAY)
                  .setSchema(new ResultSchema().setColumns(new ArrayList<>()).setColumnCount(0L))
                  .setTotalRowCount(0L));
    }

    when(apiClient.execute(any(Request.class), any()))
        .thenAnswer(
            invocationOnMock -> {
              Request req = invocationOnMock.getArgument(0, Request.class);
              if (req.getUrl().equals(STATEMENT_PATH)) {
                return response;
              } else if (req.getUrl().equals(SESSION_PATH)) {
                return new CreateSessionResponse().setSessionId(SESSION_ID);
              }
              return null;
            });
  }

  @Test
  public void testCreateSession() throws DatabricksSQLException, IOException {
    setupSessionMocks();
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContext.parse(JDBC_URL, new Properties());
    DatabricksSdkClient databricksSdkClient =
        new DatabricksSdkClient(connectionContext, statementExecutionService, apiClient);
    ImmutableSessionInfo sessionInfo =
        databricksSdkClient.createSession(warehouse, null, null, null);
    assertEquals(sessionInfo.sessionId(), SESSION_ID);
    assertEquals(sessionInfo.computeResource(), warehouse);
  }

  @Test
  public void testCreateSessionRedirect() throws DatabricksSQLException, IOException {
    // Create a DatabricksError with 307 status code to simulate the temporary redirect.
    DatabricksError redirectError =
        new DatabricksError("307", "Redirect to Thrift Client", TEMPORARY_REDIRECT_STATUS_CODE);

    // When the POST is called with the SESSION_PATH, throw the redirect error.
    when(apiClient.execute(any(Request.class), eq(CreateSessionResponse.class)))
        .thenThrow(redirectError);

    // Set up the connection context and the client.
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContext.parse(JDBC_URL, new Properties());
    DatabricksSdkClient databricksSdkClient =
        new DatabricksSdkClient(connectionContext, statementExecutionService, apiClient);

    // Assert that createSession throws a DatabricksTemporaryRedirectException.
    assertThrows(
        DatabricksTemporaryRedirectException.class,
        () -> databricksSdkClient.createSession(warehouse, null, null, null));
  }

  @Test
  public void testDeleteSession() throws DatabricksSQLException, IOException {
    String path = String.format(SESSION_PATH_WITH_ID, SESSION_ID);
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContext.parse(JDBC_URL, new Properties());
    DatabricksSdkClient databricksSdkClient =
        new DatabricksSdkClient(connectionContext, statementExecutionService, apiClient);
    ImmutableSessionInfo sessionInfo =
        ImmutableSessionInfo.builder()
            .sessionId(SESSION_ID)
            .computeResource(new Warehouse(WAREHOUSE_ID))
            .build();
    databricksSdkClient.deleteSession(sessionInfo);

    // Verify a Request with DELETE method is created and executed
    verify(apiClient)
        .execute(
            argThat(req -> req.getMethod().equals(Request.DELETE) && req.getUrl().equals(path)),
            eq(Void.class));
  }

  @Test
  public void testExecuteStatement() throws Exception {
    setupClientMocks(true, false);
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContext.parse(JDBC_URL, new Properties());
    DatabricksSdkClient databricksSdkClient =
        new DatabricksSdkClient(connectionContext, statementExecutionService, apiClient);
    DatabricksConnection connection =
        new DatabricksConnection(connectionContext, databricksSdkClient);
    connection.open();
    DatabricksStatement statement = new DatabricksStatement(connection);
    statement.setMaxRows(100);

    DatabricksResultSet resultSet =
        databricksSdkClient.executeStatement(
            STATEMENT,
            warehouse,
            sqlParams,
            StatementType.QUERY,
            connection.getSession(),
            statement);
    assertEquals(STATEMENT_ID, statement.getStatementId());
    assertNotNull(resultSet.getMetaData());

    // Verify a Request with POST method is created and executed
    verify(apiClient, atLeastOnce()).serialize(any(ExecuteStatementRequest.class));
    verify(apiClient, atLeastOnce())
        .execute(
            argThat(
                req -> req.getMethod().equals(Request.POST) && req.getUrl().equals(STATEMENT_PATH)),
            eq(ExecuteStatementResponse.class));
  }

  @Test
  public void testExecuteStatementAsync() throws Exception {
    setupClientMocks(false, true);
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContext.parse(JDBC_URL, new Properties());
    DatabricksSdkClient databricksSdkClient =
        new DatabricksSdkClient(connectionContext, statementExecutionService, apiClient);
    DatabricksConnection connection =
        new DatabricksConnection(connectionContext, databricksSdkClient);
    connection.open();
    DatabricksStatement statement = new DatabricksStatement(connection);
    statement.setMaxRows(100);

    DatabricksResultSet resultSet =
        databricksSdkClient.executeStatementAsync(
            STATEMENT, warehouse, sqlParams, connection.getSession(), statement);
    assertEquals(STATEMENT_ID, statement.getStatementId());
    assertNull(resultSet.getMetaData());

    // Verify a Request with POST method is created and executed
    verify(apiClient).serialize(any(ExecuteStatementRequest.class));
    verify(apiClient)
        .execute(
            argThat(
                req -> req.getMethod().equals(Request.POST) && req.getUrl().equals(STATEMENT_PATH)),
            eq(ExecuteStatementResponse.class));
  }

  @Test
  public void testCloseStatement() throws DatabricksSQLException, IOException {
    String path = String.format(STATEMENT_PATH_WITH_ID, STATEMENT_ID);
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContext.parse(JDBC_URL, new Properties());
    DatabricksSdkClient databricksSdkClient =
        new DatabricksSdkClient(connectionContext, statementExecutionService, apiClient);

    databricksSdkClient.closeStatement(STATEMENT_ID);

    // Verify a Request with DELETE method is created and executed
    verify(apiClient).serialize(any(CloseStatementRequest.class));
    verify(apiClient)
        .execute(
            argThat(req -> req.getMethod().equals(Request.DELETE) && req.getUrl().equals(path)),
            eq(Void.class));
  }

  @Test
  public void testCancelStatement() throws DatabricksSQLException, IOException {
    String path = String.format(CANCEL_STATEMENT_PATH_WITH_ID, STATEMENT_ID);
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContext.parse(JDBC_URL, new Properties());
    DatabricksSdkClient databricksSdkClient =
        new DatabricksSdkClient(connectionContext, statementExecutionService, apiClient);

    databricksSdkClient.cancelStatement(STATEMENT_ID);

    // Verify a Request with POST method is created and executed
    verify(apiClient).serialize(any(CancelStatementRequest.class));
    verify(apiClient)
        .execute(
            argThat(req -> req.getMethod().equals(Request.POST) && req.getUrl().equals(path)),
            eq(Void.class));
  }

  @Test
  public void testGetDatabricksConfig() throws Exception {
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContext.parse(JDBC_URL, new Properties());
    DatabricksSdkClient databricksSdkClient =
        new DatabricksSdkClient(connectionContext, statementExecutionService, apiClient);
    assertNotNull(databricksSdkClient.getDatabricksConfig());
  }

  @Test
  public void testExecuteStatementWithTimeout() throws Exception {
    // Set up connection context and client
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContext.parse(JDBC_URL, new Properties());
    DatabricksSdkClient databricksSdkClient =
        new DatabricksSdkClient(connectionContext, statementExecutionService, apiClient);
    DatabricksConnection connection =
        new DatabricksConnection(connectionContext, databricksSdkClient);

    // Mock session creation
    CreateSessionResponse sessionResponse = new CreateSessionResponse().setSessionId(SESSION_ID);
    when(apiClient.execute(any(Request.class), eq(CreateSessionResponse.class)))
        .thenReturn(sessionResponse);
    connection.open();

    // Create statement with a 10-second timeout (long enough)
    DatabricksStatement statement = new DatabricksStatement(connection);
    statement.setMaxRows(100);
    statement.setQueryTimeout(10);

    // Create statement execution mocks
    ExecuteStatementResponse executeResponse =
        new ExecuteStatementResponse()
            .setStatementId(STATEMENT_ID.toSQLExecStatementId())
            .setStatus(new StatementStatus().setState(StatementState.RUNNING));
    GetStatementResponse runningStatementResponse =
        new GetStatementResponse()
            .setStatus(new StatementStatus().setState(StatementState.RUNNING));
    GetStatementResponse successStatementResponse =
        new GetStatementResponse()
            .setStatus(new StatementStatus().setState(StatementState.SUCCEEDED));

    // Set up response sequence for execute() calls
    when(apiClient.execute(
            argThat(req -> req != null && STATEMENT_PATH.equals(req.getUrl())),
            eq(ExecuteStatementResponse.class)))
        .thenReturn(executeResponse);
    when(apiClient.execute(
            argThat(
                req ->
                    req != null
                        && req.getUrl() != null
                        && req.getUrl().contains(STATEMENT_ID.toSQLExecStatementId())),
            eq(GetStatementResponse.class)))
        .thenReturn(runningStatementResponse)
        .thenReturn(runningStatementResponse)
        .thenReturn(successStatementResponse);

    assertDoesNotThrow(
        () ->
            databricksSdkClient.executeStatement(
                STATEMENT,
                warehouse,
                sqlParams,
                StatementType.QUERY,
                connection.getSession(),
                statement));

    // Verify no cancellation occurred due to timeout
    verify(apiClient, atLeastOnce())
        .execute(
            argThat(
                req -> req.getMethod().equals(Request.POST) && req.getUrl().equals(STATEMENT_PATH)),
            eq(ExecuteStatementResponse.class));
  }

  @Test
  public void testExecuteStatementWithTimeoutExpired() throws Exception {
    // Set up connection context and client. Async exec poll interval is set to 1 second to
    // facilitate timeout
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContext.parse(
            JDBC_URL,
            new Properties() {
              {
                setProperty("asyncExecPollInterval", "1000");
              }
            });
    DatabricksSdkClient databricksSdkClient =
        spy(new DatabricksSdkClient(connectionContext, statementExecutionService, apiClient));
    DatabricksConnection connection =
        new DatabricksConnection(connectionContext, databricksSdkClient);

    // Mock session creation
    CreateSessionResponse sessionResponse = new CreateSessionResponse().setSessionId(SESSION_ID);
    when(apiClient.execute(any(Request.class), eq(CreateSessionResponse.class)))
        .thenReturn(sessionResponse);
    connection.open();

    // Create statement with a very short timeout (1 second)
    DatabricksStatement statement = new DatabricksStatement(connection);
    statement.setMaxRows(100);
    statement.setQueryTimeout(1);

    // Create statement execution mocks
    ExecuteStatementResponse executeResponse =
        new ExecuteStatementResponse()
            .setStatementId(STATEMENT_ID.toSQLExecStatementId())
            .setStatus(new StatementStatus().setState(StatementState.RUNNING));
    GetStatementResponse runningStatementResponse =
        new GetStatementResponse()
            .setStatus(new StatementStatus().setState(StatementState.RUNNING));
    GetStatementResponse successStatementResponse =
        new GetStatementResponse()
            .setStatus(new StatementStatus().setState(StatementState.SUCCEEDED));

    // Set up response sequence for execute() calls
    when(apiClient.execute(
            argThat(req -> req != null && STATEMENT_PATH.equals(req.getUrl())),
            eq(ExecuteStatementResponse.class)))
        .thenReturn(executeResponse);
    when(apiClient.execute(
            argThat(
                req ->
                    req != null
                        && req.getUrl() != null
                        && req.getUrl().contains(STATEMENT_ID.toSQLExecStatementId())),
            eq(GetStatementResponse.class)))
        .thenReturn(runningStatementResponse)
        .thenReturn(runningStatementResponse)
        .thenReturn(runningStatementResponse)
        .thenReturn(runningStatementResponse)
        .thenReturn(successStatementResponse);

    // Verify that the timeout exception (1 second) is thrown due to repeated polling, where each
    // poll occurs at an interval of 1 second
    DatabricksTimeoutException exception =
        assertThrows(
            DatabricksTimeoutException.class,
            () ->
                databricksSdkClient.executeStatement(
                    STATEMENT,
                    warehouse,
                    sqlParams,
                    StatementType.QUERY,
                    connection.getSession(),
                    statement));

    assertTrue(exception.getMessage().contains("timed-out after 1 seconds"));

    // Verify cancel was called
    verify(databricksSdkClient).cancelStatement(eq(STATEMENT_ID));
  }

  @Test
  public void testDecimalTypeWithValidPrecisionAndScale() throws DatabricksSQLException {
    BigDecimal decimalValue = new BigDecimal("123.45"); // precision: 5, scale: 2
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContext.parse(JDBC_URL, new Properties());
    DatabricksSdkClient databricksSdkClient =
        new DatabricksSdkClient(connectionContext, statementExecutionService, apiClient);
    ImmutableSqlParameter parameter =
        ImmutableSqlParameter.builder().cardinal(0).type(DECIMAL).value(decimalValue).build();

    StatementParameterListItem result = databricksSdkClient.mapToParameterListItem(parameter);

    assertEquals("DECIMAL(5,2)", result.getType());
    assertEquals("123.45", result.getValue());
  }

  @Test
  public void testDecimalTypeWithScaleGreaterThanPrecision() throws DatabricksSQLException {
    BigDecimal decimalValue = new BigDecimal("0.000123"); // scale: 6, precision: 3
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContext.parse(JDBC_URL, new Properties());
    DatabricksSdkClient databricksSdkClient =
        new DatabricksSdkClient(connectionContext, statementExecutionService, apiClient);
    ImmutableSqlParameter parameter =
        ImmutableSqlParameter.builder().cardinal(1).type(DECIMAL).value(decimalValue).build();

    StatementParameterListItem result = databricksSdkClient.mapToParameterListItem(parameter);

    assertEquals("DECIMAL(6,6)", result.getType());
    assertEquals("0.000123", result.getValue());
  }

  @Test
  public void testNonDecimalType() throws DatabricksSQLException {
    ImmutableSqlParameter parameter =
        ImmutableSqlParameter.builder().cardinal(2).type(STRING).value(TEST_STRING).build();
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContext.parse(JDBC_URL, new Properties());
    DatabricksSdkClient databricksSdkClient =
        new DatabricksSdkClient(connectionContext, statementExecutionService, apiClient);
    StatementParameterListItem result = databricksSdkClient.mapToParameterListItem(parameter);

    assertEquals("STRING", result.getType());
    assertEquals(TEST_STRING, result.getValue());
  }

  @Test
  public void testNullValue() throws DatabricksSQLException {
    ImmutableSqlParameter parameter =
        ImmutableSqlParameter.builder().cardinal(3).type(INT).value(null).build();
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContext.parse(JDBC_URL, new Properties());
    DatabricksSdkClient databricksSdkClient =
        new DatabricksSdkClient(connectionContext, statementExecutionService, apiClient);
    StatementParameterListItem result = databricksSdkClient.mapToParameterListItem(parameter);

    assertEquals("INT", result.getType());
    assertNull(result.getValue());
  }

  @Test
  public void testCreateSessionWithSSLCertificatePathError() throws Exception {

    File wrongTrustStore = File.createTempFile("wrong-trust-store", ".jks");
    wrongTrustStore.deleteOnExit();
    ConfiguratorUtilsTest.createDummyStore(
        wrongTrustStore.getAbsolutePath(), "JKS", DEFAULT_KEYSTORE_PASSWORD, "wrong-ca", false);

    SSLHandshakeException sslException =
        new SSLHandshakeException(
            "PKIX path building failed: sun.security.provider.certpath.SunCertPathBuilderException: unable to find valid certification path to requested target");

    DatabricksError sslError = mock(DatabricksError.class);
    when(sslError.getMessage()).thenReturn(sslException.getMessage());
    when(sslError.getCause()).thenReturn(sslException);

    when(apiClient.execute(any(Request.class), eq(CreateSessionResponse.class)))
        .thenThrow(sslError);

    Properties props = new Properties();
    props.setProperty("SSLTrustStore", wrongTrustStore.getAbsolutePath());
    props.setProperty("SSLTrustStorePwd", DEFAULT_KEYSTORE_PASSWORD);
    props.setProperty("SSLTrustStoreType", "JKS");

    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContext.parse(JDBC_URL, props);
    DatabricksSdkClient databricksSdkClient =
        new DatabricksSdkClient(connectionContext, statementExecutionService, apiClient);

    // Assert that createSession throws a DatabricksSQLException with actionable error message
    DatabricksSQLException exception =
        assertThrows(
            DatabricksSQLException.class,
            () -> databricksSdkClient.createSession(warehouse, null, null, null));

    String errorMessage = exception.getMessage();

    // Verify that we get the exact SSL error message
    String expectedErrorMessage =
        String.format(
            "Unable to find certification path to requested target in truststore: %s\n\n"
                + "SSL Error: PKIX path building failed: sun.security.provider.certpath.SunCertPathBuilderException: unable to find valid certification path to requested target\n\n"
                + "Details: TLS handshake failure due to TLS Certificate of server being connected is not in the configured truststore.\n\n"
                + "Next steps:\n"
                + "- Make sure that the connection string has the appropriate Databricks workspace FQDN.\n\n"
                + "- Verify the configured truststore path and make sure the required certificates are imported.\n"
                + "  .   PEM certificate chain of the warehouse endpoint can be fetched using \"openssl s_client -connect sample-host.18.azuredatabricks.net:443 -showcerts\"\n"
                + "  .   Reference KB article with troubleshooting steps.\n",
            wrongTrustStore.getAbsolutePath());
    assertEquals(expectedErrorMessage, errorMessage);

    // Clean up
    wrongTrustStore.delete();
  }

  @Test
  public void testCreateSessionWithNonSSLError() throws IOException, DatabricksSQLException {

    DatabricksError nonSSLError = new DatabricksError("500", "Some other error", 500);
    when(apiClient.execute(any(Request.class), eq(CreateSessionResponse.class)))
        .thenThrow(nonSSLError);

    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContext.parse(JDBC_URL, new Properties());
    DatabricksSdkClient databricksSdkClient =
        new DatabricksSdkClient(connectionContext, statementExecutionService, apiClient);

    DatabricksSQLException exception =
        assertThrows(
            DatabricksSQLException.class,
            () -> databricksSdkClient.createSession(warehouse, null, null, null));

    String errorMessage = exception.getMessage();
    assertEquals("Error while establishing a connection in databricks", errorMessage);
  }

  private static ImmutableSqlParameter getSqlParam(
      int parameterIndex, Object x, String databricksType) {
    return ImmutableSqlParameter.builder()
        .type(DatabricksTypeUtil.getColumnInfoType(databricksType))
        .value(x)
        .cardinal(parameterIndex)
        .build();
  }

  private StatementParameterListItem getParam(String type, String value, int ordinal) {
    return new PositionalStatementParameterListItem()
        .setOrdinal(ordinal)
        .setType(type)
        .setValue(value);
  }

  @Test
  public void testSeaSyncMetadataHeaderIsAdded() throws Exception {
    // Test that x-databricks-sea-can-run-fully-sync header is added for SEA + METADATA + sync
    setupClientMocks(true, false);
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContext.parse(JDBC_URL, new Properties());
    DatabricksSdkClient databricksSdkClient =
        new DatabricksSdkClient(connectionContext, statementExecutionService, apiClient);
    DatabricksConnection connection =
        new DatabricksConnection(connectionContext, databricksSdkClient);
    connection.open();
    DatabricksStatement statement = new DatabricksStatement(connection);

    // Execute a metadata request (synchronous)
    databricksSdkClient.executeStatement(
        "SHOW CATALOGS",
        warehouse,
        new HashMap<>(),
        StatementType.METADATA,
        connection.getSession(),
        statement);

    // Verify that the request was made with the correct header
    verify(apiClient, atLeastOnce())
        .execute(
            argThat(
                req -> {
                  Map<String, String> headers = req.getHeaders();
                  return headers != null
                      && "true".equals(headers.get("x-databricks-sea-can-run-fully-sync"));
                }),
            eq(ExecuteStatementResponse.class));
  }

  @Test
  public void testSeaSyncMetadataHeaderNotAddedForAsyncExecution() throws Exception {
    // Test that header is NOT added for async execution
    setupClientMocks(false, true);
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContext.parse(JDBC_URL, new Properties());
    DatabricksSdkClient databricksSdkClient =
        new DatabricksSdkClient(connectionContext, statementExecutionService, apiClient);
    DatabricksConnection connection =
        new DatabricksConnection(connectionContext, databricksSdkClient);
    connection.open();
    DatabricksStatement statement = new DatabricksStatement(connection);

    // Execute an async statement
    databricksSdkClient.executeStatementAsync(
        "SELECT * FROM table", warehouse, new HashMap<>(), connection.getSession(), statement);

    // Verify that the request was made WITHOUT the header
    verify(apiClient)
        .execute(
            argThat(
                req -> {
                  Map<String, String> headers = req.getHeaders();
                  return headers == null
                      || !headers.containsKey("x-databricks-sea-can-run-fully-sync");
                }),
            eq(ExecuteStatementResponse.class));
  }

  @Test
  public void testSeaSyncMetadataHeaderNotAddedForQueryType() throws Exception {
    // Test that header is NOT added for non-METADATA statement types
    setupClientMocks(true, false);
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContext.parse(JDBC_URL, new Properties());
    DatabricksSdkClient databricksSdkClient =
        new DatabricksSdkClient(connectionContext, statementExecutionService, apiClient);
    DatabricksConnection connection =
        new DatabricksConnection(connectionContext, databricksSdkClient);
    connection.open();
    DatabricksStatement statement = new DatabricksStatement(connection);

    // Execute a regular query (not metadata)
    databricksSdkClient.executeStatement(
        "SELECT * FROM table",
        warehouse,
        new HashMap<>(),
        StatementType.QUERY,
        connection.getSession(),
        statement);

    // Verify that the request was made WITHOUT the header
    verify(apiClient, atLeastOnce())
        .execute(
            argThat(
                req -> {
                  Map<String, String> headers = req.getHeaders();
                  return headers == null
                      || !headers.containsKey("x-databricks-sea-can-run-fully-sync");
                }),
            eq(ExecuteStatementResponse.class));
  }

  @Test
  public void testSeaSyncMetadataHeaderNotAddedWhenDisabled() throws Exception {
    // Test that header is NOT added when the URL parameter is disabled
    setupClientMocks(true, false);
    String urlWithDisabledFlag =
        "jdbc:databricks://sample-host.18.azuredatabricks.net:4423/default;transportMode=http;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/99999999;EnableSeaSyncMetadata=0;";
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContext.parse(urlWithDisabledFlag, new Properties());
    DatabricksSdkClient databricksSdkClient =
        new DatabricksSdkClient(connectionContext, statementExecutionService, apiClient);
    DatabricksConnection connection =
        new DatabricksConnection(connectionContext, databricksSdkClient);
    connection.open();
    DatabricksStatement statement = new DatabricksStatement(connection);

    // Execute a metadata request (should NOT add header because flag is disabled)
    databricksSdkClient.executeStatement(
        "SHOW CATALOGS",
        warehouse,
        new HashMap<>(),
        StatementType.METADATA,
        connection.getSession(),
        statement);

    // Verify that the request was made WITHOUT the header
    verify(apiClient, atLeastOnce())
        .execute(
            argThat(
                req -> {
                  Map<String, String> headers = req.getHeaders();
                  return headers == null
                      || !headers.containsKey("x-databricks-sea-can-run-fully-sync");
                }),
            eq(ExecuteStatementResponse.class));
  }
}
