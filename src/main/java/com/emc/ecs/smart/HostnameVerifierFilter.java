package com.emc.ecs.smart;

import com.sun.jersey.api.client.ClientHandlerException;
import com.sun.jersey.api.client.ClientRequest;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.filter.ClientFilter;
import com.sun.jersey.client.urlconnection.HTTPSProperties;
import org.apache.log4j.LogMF;
import org.apache.log4j.Logger;
import sun.security.x509.X500Name;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;
import java.io.IOException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.Principal;
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

        return getNext().handle(cr);
    }

    @Override
    public boolean verify(String s, SSLSession sslSession) {
        try {
            for(Certificate cert : sslSession.getPeerCertificates()) {
                LogMF.debug(l4j, "Cert type: {0}", cert.getType());
                X509Certificate xcert = (X509Certificate)cert;
                Principal p = xcert.getSubjectDN();
                String dn = xcert.getSubjectDN().getName();
                LogMF.debug(l4j, "Cert DN: {0}, class: {1}", dn, xcert.getSubjectDN().getClass());

                // TODO: X500Name is a proprietary (internal) class.  The code says to see X500Principal in
                // javax.security.auth.x500 but that does not seem to be a suitable replacement for parsing the
                // CN out of the DN.
                if(p instanceof X500Name) {
                    X500Name xn = (X500Name)p;
                    LogMF.debug(l4j, "Cert CN: {0}", xn.getCommonName());
                    if(xn.getCommonName().equals(hostname)) {
                        LogMF.debug(l4j, "CN: {0} Matches hostname {1}, accepting cert", xn.getCommonName(), hostname);
                        return true;
                    }
                }
                try {
                    for(List<?> sans : xcert.getSubjectAlternativeNames()) {
                        for(Object san : sans) {
                            LogMF.debug(l4j, "SAN: {0} Class: {1}", san.toString(), san.getClass());
                            String ssan = san.toString();
                            if(ssan.equals(hostname)) {
                                LogMF.debug(l4j, "SAN {0} matches {1}, accepting cert", ssan, hostname);
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
        } catch (IOException e) {
            l4j.error("Could not parse X500Name", e);
        }

        return false;
    }
}
