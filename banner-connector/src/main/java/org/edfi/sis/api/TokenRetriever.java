package org.edfi.sis.api;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.*;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;

import javax.naming.AuthenticationException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

@Component
public class TokenRetriever {

    @Value( "${oauth.token.url}" )
    private String oauthUrl;
    @Value( "${oauth.client.id}" )
    private String clientKey;
    @Value( "${oauth.client.secret}" )
    private String clientSecret;

    public TokenRetriever() {

    }

    public TokenRetriever(String oauthUrl, String clientKey, String clientSecret) {
        this.oauthUrl = oauthUrl;
        this.clientKey = clientKey;
        this.clientSecret = clientSecret;
    }

    public String obtainNewBearerToken() throws AuthenticationException {
        CredentialsProvider provider = new BasicCredentialsProvider();
        String clientCredentials = Base64.getEncoder()
                .encodeToString(new String(clientKey + ":" + clientSecret).getBytes());

        HttpClient client = HttpClientBuilder.create().setDefaultCredentialsProvider(provider).build();

        HttpPost httpPost = new HttpPost(oauthUrl);
        List<NameValuePair> params = new ArrayList<>();
        httpPost.addHeader(org.apache.http.HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_FORM_URLENCODED_VALUE);
        httpPost.addHeader(org.apache.http.HttpHeaders.AUTHORIZATION, "Basic " + clientCredentials);
        params.add(new BasicNameValuePair("grant_type", "client_credentials"));
        try {
            httpPost.setEntity(new UrlEncodedFormEntity(params));
            HttpResponse response = client.execute(httpPost);
            StatusLine statusLine = response.getStatusLine();
            if (statusLine.getStatusCode() == HttpStatus.OK.value()) {
                HttpEntity entity = response.getEntity();
                String responseData = EntityUtils.toString(entity, Consts.UTF_8);
                ObjectMapper objectMapper = new ObjectMapper();
                AccessToken acessTokenObject = objectMapper.readValue(responseData, AccessToken.class);
                String accessToken = acessTokenObject.getAccessToken();
                return accessToken;
            }
        } catch (IOException e) {
            throw new AuthenticationException(e.getMessage());
        }

        return null;
    }
}