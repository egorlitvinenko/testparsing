package org.egorlitvinenko.testdisruptor.clickhousestream.ch;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Egor Litvinenko
 */
public class ClickhouseHttp {

    private final Map<String, URI> preparedUris = new ConcurrentHashMap<>();
    private final HttpClient httpClient;
    private final ClickHouseProperties properties;

    public ClickhouseHttp(String url) {
        this(url, new ClickHouseProperties());
    }

    public ClickhouseHttp(String url, ClickHouseProperties properties) {
        try {
            this.properties = ClickhouseJdbcUrlParser.parse(url, properties.asProperties());
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException(url, e);
        }
        try {
            this.httpClient = new ClickHouseHttpClientBuilder(properties).buildClient();
        } catch (Exception e) {
            throw new IllegalArgumentException("Client", e);
        }
    }


    public void sendInsert(HttpEntity content, String sql) throws Exception {
        if (null == sql || sql.isEmpty()) {
            throw new IllegalArgumentException(
                    "Query must be like 'INSERT INTO [db.]table [(c1, c2, c3)]" +
                            "Got: " + sql);
        }

        // echo -ne '10\n11\n12\n' | POST 'http://localhost:8123/?query=INSERT INTO t FORMAT TabSeparated'
        HttpEntity entity = null;
        try {
            URI uri = buildRequestUri(sql);

            HttpPost httpPost = new HttpPost(uri);
            if (properties.isDecompress()) {
                httpPost.setEntity(new LZ4EntityWrapper(content, properties.getMaxCompressBufferSize()));
            } else {
                httpPost.setEntity(content);
            }
            HttpResponse response = httpClient.execute(httpPost);
            entity = response.getEntity();
            if (response.getStatusLine().getStatusCode() != HttpURLConnection.HTTP_OK) {
                String chMessage;
                try {
                    chMessage = EntityUtils.toString(response.getEntity());
                } catch (IOException e) {
                    chMessage = "error while read response " + e.getMessage();
                }
                throw ClickHouseExceptionSpecifier.specify(chMessage, properties.getHost(), properties.getPort());
            }
        } catch (ClickHouseException e) {
            throw e;
        } catch (Exception e) {
            throw ClickHouseExceptionSpecifier.specify(e, properties.getHost(), properties.getPort());
        } finally {
            EntityUtils.consumeQuietly(entity);
        }
    }

    private URI buildRequestUri(String sql) {
        URI result = preparedUris.get(sql);
        if (result == null) {
            try {
                List<NameValuePair> queryParams = getUrlQueryParams(
                        sql
                );
                result = new URIBuilder()
                        .setScheme(properties.getSsl() ? "https" : "http")
                        .setHost(properties.getHost())
                        .setPort(properties.getPort())
                        .setPath("/")
                        .setParameters(queryParams)
                        .build();
            } catch (URISyntaxException e) {
                throw new IllegalStateException("illegal configuration of db", e);
            }
            preparedUris.put(sql, result);
        }
        return result;
    }

    private List<NameValuePair> getUrlQueryParams(
            String sql
    ) {
        List<NameValuePair> result = new ArrayList<NameValuePair>();

        if (sql != null) {
            result.add(new BasicNameValuePair("query", sql));
        }

        return result;
    }

}
