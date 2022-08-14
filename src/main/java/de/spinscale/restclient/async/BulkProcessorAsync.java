package de.spinscale.restclient.async;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import de.spinscale.restclient.Product;
import de.spinscale.restclient.ProductDeserializer;
import de.spinscale.restclient.ProductSerializer;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This is a bulk processor returning a PlainActionFuture
 *
 * Idea of this implementation is to use a bulk processor, when updating a lot of single documents
 * This way you prevent those updates at the expense of increased wait times (up to one second maximum)
 *
 * Limitations of this is the fact that an id can only be written once at the same time,
 * as we use the document id as a map key
 */
public class BulkProcessorAsync implements AutoCloseable {

    private final BulkProcessor bulkProcessor;
    private final String index;
    private final ObjectMapper mapper;
    private final Map<String, PlainActionFuture<IndexResponse>> futures;

    public BulkProcessorAsync(RestHighLevelClient client, String index) {
        this.bulkProcessor = BulkProcessor.builder(
                (request, bulkListener) -> client.bulkAsync(request, RequestOptions.DEFAULT, bulkListener),
                new FutureNotificationListener(), "bulk-processor")
                .setFlushInterval(TimeValue.timeValueSeconds(1))
                .setBulkActions(100)
                .build();
        this.index = index;
        this.mapper = new ObjectMapper();
        SimpleModule module = new SimpleModule();
        module.addSerializer(Product.class, new ProductSerializer());
        module.addDeserializer(Product.class, new ProductDeserializer());
        mapper.registerModule(module);
        futures = new ConcurrentHashMap<>();
    }

    public PlainActionFuture<IndexResponse> save(Product product) throws Exception {
        Objects.requireNonNull(product.getId(), "ID cannot be null");
        final PlainActionFuture<IndexResponse> future = PlainActionFuture.newFuture();
        final IndexRequest indexRequest = indexRequest(product);
        final PlainActionFuture<IndexResponse> existingFuture = futures.putIfAbsent(indexRequest.id(), future);
        if (existingFuture != null) {
            future.onFailure(new RuntimeException("product[" + product.getId() + "] is already being stored"));
        } else {
            bulkProcessor.add(indexRequest);
        }
        return future;
    }

    private IndexRequest indexRequest(Product product) throws IOException {
        final byte[] bytes = mapper.writeValueAsBytes(product);
        final IndexRequest request = new IndexRequest(index);
        if (product.getId() != null) {
            request.id(product.getId());
        }
        request.source(bytes, XContentType.JSON);
        return request;
    }

    @Override
    public void close() {
        bulkProcessor.close();
    }

    public class FutureNotificationListener implements BulkProcessor.Listener {

        @Override
        public void beforeBulk(long executionId, BulkRequest request) {
        }

        @Override
        public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
            for (int i = 0; i < request.numberOfActions(); i++) {
                final PlainActionFuture<IndexResponse> future = futures.remove(request.requests().get(i).id());
                final IndexResponse indexResponse = response.getItems()[i].getResponse();
                future.onResponse(indexResponse);
            }
        }

        @Override
        public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
            for (int i = 0; i < request.numberOfActions(); i++) {
                final PlainActionFuture<IndexResponse> future = futures.remove(request.requests().get(i).id());
                future.onFailure(new Exception(failure));
            }
        }
    }
}
