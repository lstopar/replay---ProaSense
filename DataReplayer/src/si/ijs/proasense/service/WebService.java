package si.ijs.proasense.service;


import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.WebResource.Builder;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.sun.jersey.core.util.MultivaluedMapImpl;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;

/**
 * A universal web service client.
 */
public class WebService {

    private static Client client = Client.create(new DefaultClientConfig());

    public enum Method {
        GET,
        POST
    }

    /*
     * A ThreadPoolExecutor with a dynamic number of threads ranging from 0 to 
     * Configuration.DISTRIBUTE_THREADS. A thread is destroyed after being idle for 1 minute and
     * the queue is unlimited.
     */
    private static final ExecutorService executor = new ThreadPoolExecutor(0, 1, 60L, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>());

    /**
     * Fetches a response from the specified URL
     *
     * @param url the URL
     * @param params optional GET or POST parameters
     * @param body optional the request body
     * @param mediaType optional default is MediaType.WILDCARD
     * @param contentType optional
     * @param method the HTTP method to use, currently only GET and POST supported
     * @return
     */
    public static String fetchUrl(String url, Map<String, String> params, String body, String mediaType, String contentType, Method method) {
        WebResource resource = client.resource(url);
        
        // build parameters
        if (params != null && !params.isEmpty()) {
            MultivaluedMap<String, String> formParams = new MultivaluedMapImpl();
            for (String key : params.keySet())
                formParams.add(key, params.get(key));
            resource = resource.queryParams(formParams);
        }

        Builder builder = resource.accept(mediaType == null ? MediaType.WILDCARD : mediaType);

        if (contentType != null)
            builder = builder.type(contentType);

        // get response
        ClientResponse response;
        switch (method) {
            case GET:
                response = builder.get(ClientResponse.class);
                break;
            case POST:
                if (body != null)
                    response = builder.post(ClientResponse.class, body);
                else
                    response = builder.post(ClientResponse.class);
                break;
            default:
                System.out.println("Unknown method: " + method);
                return null;
        }

        if (response.getStatus() == 200) {
            // success, now parse the response
            String responseStr = response.getEntity(String.class);

//            log.debug("Received response:\n" + responseStr);
            return responseStr;
        } else if (response.getStatus() == 204) {
            return "";
        } else {
            System.out.println("Failed to fetch resource, status code: " + response.getStatus());
            return null;
        }
    }

    /**
     * Sends the request at some future time, ignores the result.
     *
     * @param url the URL
     * @param params optional GET or POST parameters
     * @param body optional the request body
     * @param mediaType optional default is MediaType.WILDCARD
     * @param contentType optional
     * @param method the HTTP method to use, currently only GET and POST supported
     */
    public static void sendAndForget(final String url, final Map<String, String> params, final String body,
            final String mediaType, final String contentType, final Method method) {
        executor.execute(new Runnable() {
            @Override
            public void run() {
            	fetchUrl(url, params, body, mediaType, contentType, method);
            }
        });
    }
}
