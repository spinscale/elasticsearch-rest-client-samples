package de.spinscale.restclient;

import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.ssl.SSLContexts;

import javax.net.ssl.SSLContext;
import javax.net.ssl.X509TrustManager;
import java.io.ByteArrayInputStream;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;

public class SslUtils {

    public static SSLContext trustAllContext() {
            X509TrustManager tm = new X509TrustManager() {
                @Override
                public void checkClientTrusted(X509Certificate[] certs, String authType) {
                    // Accept anything
                }

                @Override
                public void  checkServerTrusted(X509Certificate[] certs, String authType) {
                    // Accept anything
                }

                @Override
                public X509Certificate[] getAcceptedIssuers() {
                    return new X509Certificate[0];
                }
            };

            try {
                SSLContext ctx = SSLContext.getInstance("SSL");
                ctx.init(null, new X509TrustManager[] { tm }, null);
                return ctx;
            } catch (Exception e) {
                // An exception here means SSL is not supported, which is unlikely
                throw new RuntimeException(e);
            }
    }

    public static SSLContext createContextFromCaCert(byte[] certAsBytes) {
        try {
            CertificateFactory factory = CertificateFactory.getInstance("X.509");
            Certificate trustedCa = factory.generateCertificate(new ByteArrayInputStream(certAsBytes));
            KeyStore trustStore = KeyStore.getInstance("pkcs12");
            trustStore.load(null, null);
            trustStore.setCertificateEntry("ca", trustedCa);
            SSLContextBuilder sslContextBuilder = SSLContexts.custom().loadTrustMaterial(trustStore, null);
            return sslContextBuilder.build();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
