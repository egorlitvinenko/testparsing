package org.egorlitvinenko.testdisruptor.clickhousestream.ch;

import org.apache.http.HeaderElement;
import org.apache.http.HeaderElementIterator;
import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.config.ConnectionConfig;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.ConnectionKeepAliveStrategy;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.message.BasicHeaderElementIterator;
import org.apache.http.protocol.HTTP;
import org.apache.http.protocol.HttpContext;

import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.security.*;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;


public class ClickHouseHttpClientBuilder {

    private final ClickHouseProperties properties;

    public ClickHouseHttpClientBuilder(ClickHouseProperties properties) {
        this.properties = properties;
    }

    public CloseableHttpClient buildClient() throws Exception {
        return HttpClientBuilder.create()
                .setConnectionManager(getConnectionManager())
                .setKeepAliveStrategy(createKeepAliveStrategy())
                .setDefaultConnectionConfig(getConnectionConfig())
                .setDefaultRequestConfig(getRequestConfig())
                .disableContentCompression() // gzip здесь ни к чему. Используется lz4 при compress=1
                .build();
    }

    private PoolingHttpClientConnectionManager getConnectionManager()
        throws CertificateException, NoSuchAlgorithmException, KeyStoreException, KeyManagementException, IOException {
        RegistryBuilder<ConnectionSocketFactory> registry = RegistryBuilder.<ConnectionSocketFactory>create()
          .register("http", PlainConnectionSocketFactory.getSocketFactory());

        if (properties.getSsl()) {
            registry.register("https", new SSLConnectionSocketFactory(getSSLContext()));
        }

        //noinspection resource
        PoolingHttpClientConnectionManager connectionManager = new PoolingHttpClientConnectionManager(
            registry.build(),
            null,
            null,
            new IpVersionPriorityResolver(),
            properties.getTimeToLiveMillis(),
            TimeUnit.MILLISECONDS
        );

        connectionManager.setDefaultMaxPerRoute(properties.getDefaultMaxPerRoute());
        connectionManager.setMaxTotal(properties.getMaxTotal());
        return connectionManager;
    }

    private ConnectionConfig getConnectionConfig() {
        return ConnectionConfig.custom()
                .setBufferSize(properties.getApacheBufferSize())
                .build();
    }

    private RequestConfig getRequestConfig() {
        return RequestConfig.custom()
                .setSocketTimeout(properties.getSocketTimeout())
                .setConnectTimeout(properties.getConnectionTimeout())
                .build();
    }

    private ConnectionKeepAliveStrategy createKeepAliveStrategy() {
        return new ConnectionKeepAliveStrategy() {
            @Override
            public long getKeepAliveDuration(HttpResponse httpResponse, HttpContext httpContext) {
                // in case of errors keep-alive not always works. reset connection just in case
                if (httpResponse.getStatusLine().getStatusCode() != HttpURLConnection.HTTP_OK) {
                    return -1;
                }
                HeaderElementIterator it = new BasicHeaderElementIterator(
                        httpResponse.headerIterator(HTTP.CONN_DIRECTIVE));
                while (it.hasNext()) {
                    HeaderElement he = it.nextElement();
                    String param = he.getName();
                    //String value = he.getValue();
                    if (param != null && param.equalsIgnoreCase(HTTP.CONN_KEEP_ALIVE)) {
                        return properties.getKeepAliveTimeout();
                    }
                }
                return -1;
            }
        };
    }

  private SSLContext getSSLContext()
      throws CertificateException, NoSuchAlgorithmException, KeyStoreException, IOException, KeyManagementException {
      TrustManager[] tms;
      if(properties.getSslMode().equals("none")) {
          tms = new TrustManager[]{new NonValidatingTrustManager()};
      } else if (properties.getSslMode().equals("strict")){
        TrustManagerFactory tmf = TrustManagerFactory
            .getInstance(TrustManagerFactory.getDefaultAlgorithm());

        tmf.init(getKeyStore());
        tms = tmf.getTrustManagers();
      } else {
          throw new IllegalArgumentException("unknown ssl mode '"+ properties.getSslMode() +"'");
      }

      SSLContext ctx = SSLContext.getInstance("TLS");
      ctx.init(new KeyManager[]{}, tms, new SecureRandom());

      return ctx;
  }

  private KeyStore getKeyStore()
      throws NoSuchAlgorithmException, IOException, CertificateException, KeyStoreException {
      KeyStore ks;
      try {
          ks = KeyStore.getInstance("jks");
          ks.load(null, null); // needed to initialize the key store
      } catch (KeyStoreException e) {
          throw new NoSuchAlgorithmException("jks KeyStore not available");
      }

      InputStream caInputStream;
      try {
        caInputStream = new FileInputStream(properties.getSslRootCertificate());
      } catch (FileNotFoundException ex) {
          ClassLoader cl = Thread.currentThread().getContextClassLoader();
          caInputStream = cl.getResourceAsStream(properties.getSslRootCertificate());
          if(caInputStream == null) {
              throw new IOException(
                  "Could not open SSL/TLS root certificate file '" + properties
                      .getSslRootCertificate() + "'", ex);
          }
      }
      CertificateFactory cf = CertificateFactory.getInstance("X.509");
      Iterator<? extends Certificate> caIt = cf.generateCertificates(caInputStream).iterator();
      for (int i = 0; caIt.hasNext(); i++) {
        ks.setCertificateEntry("cert" + i, caIt.next());
      }

      return ks;
  }

}
