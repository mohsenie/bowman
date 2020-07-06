/*
 * Copyright 2016 Black Pepper Software
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package uk.co.blackpepper.bowman;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.hateoas.Resource;
import org.springframework.hateoas.Resources;
import org.springframework.http.*;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;

import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.Optional;

class RestOperations {

    private final RestTemplate restTemplate;

    private final ObjectMapper objectMapper;
    private ClientFactoryCallBackInterface callbackInterface;
    private static final Logger logger = LoggerFactory.getLogger(RestOperations.class);
    private final CacheCommandsInterface cacheManagerInterface;
    private final boolean disableCache;

    RestOperations(RestTemplate restTemplate, ObjectMapper objectMapper, CacheCommandsInterface cacheManagerInterface, boolean disableCache) {
        this.restTemplate = restTemplate;
        this.objectMapper = objectMapper;
        this.cacheManagerInterface = cacheManagerInterface;
        this.disableCache = disableCache;
    }

    public void setCallbackInterface(ClientFactoryCallBackInterface callbackInterface) {
        this.callbackInterface = callbackInterface;
    }

    public <T> Resource<T> getResource(URI uri, Class<T> entityType) {
        ObjectNode node;

        try {
            if(!disableCache) {
                node = getCachedObject(uri);
            }else {
                node = restTemplate.getForObject(uri, ObjectNode.class);
            }
        } catch (HttpClientErrorException exception) {
            if (exception.getStatusCode() == HttpStatus.NOT_FOUND) {
                return null;
            }

            throw exception;
        }

        JavaType targetType = objectMapper.getTypeFactory().constructParametricType(Resource.class, entityType);

        return objectMapper.convertValue(node, targetType);
    }

    public <T> Resources<Resource<T>> getResources(URI uri, Class<T> entityType) {
        ObjectNode node;

        try {
            if(!disableCache) {
                logger.info("Cache is enabled");
                node = getCachedObject(uri);
            }else {
                logger.info("Cache is is disabled");
                node = restTemplate.getForObject(uri, ObjectNode.class);
            }
            JsonNode pageNode = node.get("page");
            JsonNode linksNode = node.get("_links");

            Pagination pagination = Optional.ofNullable(pageNode).isPresent() ? objectMapper.convertValue(pageNode, Pagination.class) : null;

            if (Optional.ofNullable(pagination).isPresent()) {
                JsonNode firstNode = Optional.ofNullable(linksNode).isPresent() ? linksNode.get("first") : null;
                JsonNode nextNode = Optional.ofNullable(linksNode).isPresent() ? linksNode.get("next") : null;
                JsonNode lastNode = Optional.ofNullable(linksNode).isPresent() ? linksNode.get("last") : null;

                pagination.setFirstPage(Optional.ofNullable(firstNode).isPresent() ? firstNode.get("href").asText() : null);
                pagination.setNextPage(Optional.ofNullable(nextNode).isPresent() ? nextNode.get("href").asText() : null);
                pagination.setLastPage(Optional.ofNullable(lastNode).isPresent() ? lastNode.get("href").asText() : null);
            }
            if (callbackInterface != null && pagination != null) {
                callbackInterface.setPagination(pagination);
            }
        } catch (HttpClientErrorException exception) {
            if (exception.getStatusCode() == HttpStatus.NOT_FOUND) {
                return Resources.wrap(Collections.emptyList());
            }

            throw exception;
        }

        JavaType innerType = objectMapper.getTypeFactory().constructParametricType(Resource.class, entityType);
        JavaType targetType = objectMapper.getTypeFactory().constructParametricType(Resources.class, innerType);

        return objectMapper.convertValue(node, targetType);
    }

    public URI postForId(URI uri, Object object) {
        return restTemplate.postForLocation(uri, object);
    }

    public void put(URI uri, Object object) {
        restTemplate.put(uri, object);
    }

    public void delete(URI uri) {
        restTemplate.delete(uri);
    }

    public <T> Resource<T> patchForResource(URI uri, Object patch, Class<T> entityType) {
        ObjectNode node;

        node = restTemplate.patchForObject(uri, patch, ObjectNode.class);
        if (node == null) {
            return null;
        }

        JavaType targetType = objectMapper.getTypeFactory().constructParametricType(Resource.class, entityType);

        return objectMapper.convertValue(node, targetType);
    }

    RestTemplate getRestTemplate() {
        return restTemplate;
    }

    ObjectMapper getObjectMapper() {
        return objectMapper;
    }

    public ObjectNode getCachedObject(URI uri) {
        ObjectNode node;

        String etag = cacheManagerInterface.getEatgForUrl(uri.toString());

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        if(etag != null) {
            headers.add("If-None-Match", etag);
        }
        MultiValueMap<String, String> map = new LinkedMultiValueMap<>();
        HttpEntity<MultiValueMap<String, String>> requestEntity = new HttpEntity<>(map, headers);

        ResponseEntity<ObjectNode> result = restTemplate.exchange(uri, HttpMethod.GET, requestEntity, ObjectNode.class);

        if(result == null){
            logger.warn("cache is going to return null object because rest template has returned null object");
            throw new HttpClientErrorException(HttpStatus.NOT_FOUND, "Resource not found");
        }
        node = result.getBody();

        if (result.getStatusCode() == HttpStatus.NOT_MODIFIED) {
            //if response status is 303 then try to load the contents from cache
            try {
                node = objectMapper.readValue(cacheManagerInterface.getCacheForUrl(uri.toString()), ObjectNode.class);
            } catch (IOException e) {
                logger.error("failed reading object bytes from cache : " + e.getMessage());
                logger.info("calling cache manager to evict cache for the url");
                cacheManagerInterface.evictCacheForUrl(uri.toString());
            }
        } else {
            try {
                logger.info("calling cache manager to persist object.");
                cacheManagerInterface.writeCacheForUrl(uri.toString(), result.getHeaders().getETag(), objectMapper.writeValueAsBytes(node));
            } catch (IOException e) {
                logger.error("failed converting response object to byte array : " + e.getMessage());
            }
        }
        return node;
    }
}
