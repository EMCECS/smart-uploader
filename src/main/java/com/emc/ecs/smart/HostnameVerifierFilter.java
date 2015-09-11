package com.emc.ecs.smart;

import com.sun.jersey.api.client.ClientHandlerException;
import com.sun.jersey.api.client.ClientRequest;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.filter.ClientFilter;
import com.sun.jersey.client.urlconnection.HTTPSProperties;
import org.apache.log4j.LogMF;
import org.apache.log4j.Logger;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.Certificate;
import java.security.cert.CertificateParsingException;
import java.security.cert.X509Certificate;
import java.util.List;

/**
 * Adds in a HostnameVerifier to handle instances where we're manually redirecting from hostname to IP.  We'll verify
 * the original hostname instead of the IP.
 */
public class HostnameVerifierFilter extends ClientFilter implements HostnameVerifier {
    private static final Logger l4j = Logger.getLogger(HostnameVerifierFilter.class);
    private String hostname;

    public HostnameVerifierFilter(String hostname) {
        this.hostname = hostname;
    }
    @Override
    public ClientResponse handle(ClientRequest cr) throws ClientHandlerException {
        if(cr.getURI().getScheme().equals("https")) {
            if(!cr.getURI().getHost().equals(hostname)) {
                LogMF.debug(l4j, "Request hostname {0} does not match {1}.  Injecting custom hostname verifier",
                        cr.getURI().getHost(), hostname);
                try {
                    SSLContext sslcontext = SSLContext.getInstance("TLS");
                    sslcontext.init(null, null, null);
                    cr.getProperties().put(HTTPSProperties.PROPERTY_HTTPS_PROPERTIES, new HTTPSProperties(this, sslcontext));
                } catch (NoSuchAlgorithmException e) {
                    throw new ClientHandlerException("Could not install hostname verifier", e);
                } catch (KeyManagementException e) {
                    throw new ClientHandlerException("Could not initialize TLS");
                }
            }
        }

        return getNext().handle(cr);
    }

    @Override
    public boolean verify(String s, SSLSession sslSession) {
        try {
            for(Certificate cert : sslSession.getPeerCertificates()) {
                LogMF.debug(l4j, "Cert type: {0}", cert.getType());
                X509Certificate xcert = (X509Certificate)cert;
                String dn = xcert.getSubjectDN().getName();
                LogMF.debug(l4j, "Cert DN: {0}", dn);
                if(dn.equals(hostname)) {
                    return true;
                }
                try {
                    for(List<?> sans : xcert.getSubjectAlternativeNames()) {
                        for(Object san : sans) {
                            LogMF.debug(l4j, "SAN: ", san);
                            String ssan = san.toString();
                            if(ssan.equals(hostname)) {
                                return true;
                            }
                        }
                    }
                } catch (CertificateParsingException e) {
                    l4j.error("Could not parse certificate", e);
                    return false;
                }
            }
        } catch (SSLPeerUnverifiedException e) {
            l4j.error("Could not get certificates", e);
        }

        return false;
    }
}
