package de.spinscale.restclient.async;

import de.spinscale.restclient.Product;
import org.apache.http.HttpHost;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.rest.RestStatus;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.elasticsearch.ElasticsearchContainer;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

// test class for the just for fun bulk processor
class BulkProcessorAsyncTests {

    private static final ElasticsearchContainer container =
            new ElasticsearchContainer("docker.elastic.co/elasticsearch/elasticsearch:7.10.0").withExposedPorts(9200);

    private static final String INDEX = "my_index";
    private static RestHighLevelClient client;
    private static BulkProcessorAsync bulkProcessorAsync;

    @BeforeAll
    public static void startElasticsearchCreateLocalClient() {
        container.start();

        HttpHost host = new HttpHost("localhost", container.getMappedPort(9200));
        final RestClientBuilder builder = RestClient.builder(host);
        client = new RestHighLevelClient(builder);
        bulkProcessorAsync = new BulkProcessorAsync(client, INDEX);
    }

    @AfterAll
    public static void closeResources() throws Exception {
        client.close();
        bulkProcessorAsync.close();
    }

    @AfterEach
    public void deleteProductIndex() throws Exception {
        try {
            client.indices().delete(new DeleteIndexRequest(INDEX), RequestOptions.DEFAULT);
        } catch (ElasticsearchStatusException e) {
        }
    }

    @Test
    public void testAsyncSave() throws Exception {
        Product product = new Product();
        product.setId("my_id");
        product.setName("my_name");
        product.setDescription("my fancy description");
        product.setPrice(12.34);
        product.setStockAvailable(1234);
        final PlainActionFuture<IndexResponse> future = bulkProcessorAsync.save(product);
        final IndexResponse indexResponse = future.get(10, TimeUnit.SECONDS);

        assertThat(indexResponse.status()).isEqualTo(RestStatus.CREATED);

        // retrieve document
        final GetResponse getResponse = client.get(new GetRequest(INDEX, "my_id"), RequestOptions.DEFAULT);
        assertThat(getResponse.isExists()).isTrue();
    }

}
