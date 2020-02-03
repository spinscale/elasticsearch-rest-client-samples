package de.spinscale.restclient;

import org.apache.http.HttpHost;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Node;
import org.elasticsearch.client.NodeSelector;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.elasticsearch.ElasticsearchContainer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

// more logging everything: -Dorg.slf4j.simpleLogger.defaultLogLevel=DEBUG
// more logging ES client only:
public class ElasticsearchTests {

    private static final ElasticsearchContainer container =
            new ElasticsearchContainer("docker.elastic.co/elasticsearch/elasticsearch:7.5.2").withExposedPorts(9200);
    private static final NodeSelector INGEST_NODE_SELECTOR = nodes -> {
        final Iterator<Node> iterator = nodes.iterator();
        while (iterator.hasNext()) {
            Node node = iterator.next();
            // roles may be null if we don't know, thus we keep the node in then...
            if (node.getRoles() != null && node.getRoles().isIngest() == false) {
                iterator.remove();
            }
        }
    };
    public static final String INDEX = "my_index";
    private static RestHighLevelClient client;
    private static ProductServiceImpl productService;

    @BeforeAll
    public static void startElasticsearch() {
        container.start();

        HttpHost host = new HttpHost("localhost", container.getMappedPort(9200));
        final RestClientBuilder builder = RestClient.builder(host);
        builder.setNodeSelector(INGEST_NODE_SELECTOR);
        client = new RestHighLevelClient(builder);
        productService = new ProductServiceImpl(INDEX, host);
    }

    @AfterAll
    public static void closeResources() throws Exception {
        client.close();
        productService.close();
    }

    @AfterEach
    public void deleteProductIndex() throws Exception {
        try {
            client.indices().delete(new DeleteIndexRequest(INDEX), RequestOptions.DEFAULT);
        } catch (ElasticsearchStatusException e) {
        }
    }

    @Test
    public void testClusterVersion() throws Exception {
        final ClusterHealthResponse response = client.cluster().health(new ClusterHealthRequest(), RequestOptions.DEFAULT);
        // check for yellow or green cluster health
        assertThat(response.getStatus()).isNotEqualTo(ClusterHealthStatus.RED);

        // async party!
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<ClusterHealthResponse> reference = new AtomicReference<>();
        final ActionListener<ClusterHealthResponse> listener = ActionListener.wrap(
                r -> { reference.set(r); latch.countDown(); },
                e -> { e.printStackTrace(); latch.countDown(); });
        client.cluster().healthAsync(new ClusterHealthRequest(), RequestOptions.DEFAULT, listener);
        latch.await(10, TimeUnit.SECONDS);
        assertThat(reference.get().getStatus()).isNotEqualTo(ClusterHealthStatus.RED);
    }

    @Test
    public void indexProductWithoutId() throws Exception {
        Product product = createProducts(1).get(0);
        product.setId(null);
        assertThat(product.getId()).isNull();

        productService.save(product);

        assertThat(product.getId()).isNotNull();
    }

    @Test
    public void indexProductWithId() throws Exception {
        Product product = createProducts(1).get(0);
        assertThat(product.getId()).isEqualTo("0");

        productService.save(product);

        assertThat(product.getId()).isEqualTo("0");
    }

    @Test
    public void testFindProductById() throws Exception {
        productService.save(createProducts(3));

        final Product product1 = productService.findById("0");
        assertThat(product1.getId()).isEqualTo("0");
        final Product product2 = productService.findById("1");
        assertThat(product2.getId()).isEqualTo("1");
        final Product product3 = productService.findById("2");
        assertThat(product3.getId()).isEqualTo("2");
    }

    @Test
    public void testSearch() throws Exception {
        productService.save(createProducts(10));
        client.indices().refresh(new RefreshRequest(INDEX), RequestOptions.DEFAULT);

        final Page<Product> page = productService.search("9");
        assertThat(page.get()).hasSize(1);
        assertThat(page.get()).first().extracting("id").isEqualTo("9");
    }

    @Test
    public void testPagination() throws Exception {
        productService.save(createProducts(21));
        client.indices().refresh(new RefreshRequest(INDEX), RequestOptions.DEFAULT);

        // matches all products
        final Page<Product> page = productService.search("name");
        assertThat(page.get()).hasSize(10);
        final Page<Product> secondPage = productService.next(page);
        assertThat(page.get()).hasSize(10);
        List<String> firstPageIds = page.get().stream().map(Product::getId).collect(Collectors.toList());
        List<String> secondPageIds = secondPage.get().stream().map(Product::getId).collect(Collectors.toList());
        assertThat(firstPageIds).isNotEqualTo(secondPageIds);
        final Page<Product> thirdPage = productService.next(secondPage);
        assertThat(thirdPage.get()).hasSize(1);
    }

    @Test
    public void testSearchAfter() throws Exception {
        productService.save(createProducts(21));
        client.indices().refresh(new RefreshRequest(INDEX), RequestOptions.DEFAULT);

        SearchRequest searchRequest = new SearchRequest(INDEX);
        searchRequest.source().query(QueryBuilders.matchQuery("name", "Name"));
        searchRequest.source().sort(SortBuilders.fieldSort("price").order(SortOrder.DESC));
        final SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
        final List<String> ids = Arrays.stream(response.getHits().getHits()).map(SearchHit::getId).collect(Collectors.toList());

        // first search after
        SearchRequest searchAfterRequest = new SearchRequest(INDEX);
        searchAfterRequest.source().query(QueryBuilders.matchQuery("name", "Name"));
        searchAfterRequest.source().sort(SortBuilders.fieldSort("price").order(SortOrder.DESC));
        SearchHit lastHit = response.getHits().getHits()[response.getHits().getHits().length-1];
        searchAfterRequest.source().searchAfter(lastHit.getSortValues());
        final SearchResponse searchAfterResponse = client.search(searchAfterRequest, RequestOptions.DEFAULT);
        final List<String> searchAfterIds = Arrays.stream(searchAfterResponse.getHits().getHits()).map(SearchHit::getId).collect(Collectors.toList());

        assertThat(ids).isNotEqualTo(searchAfterIds);
    }

    private List<Product> createProducts(int count) {
        List<Product> products = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            Product product = new Product();
            product.setId(i + "");
            product.setName("Name of " + i + " product");
            product.setDescription("Description of " + i + " product");
            product.setPrice(i * 1.2);
            product.setStockAvailable(i * 10);
            products.add(product);
        }

        return products;
    }

//    @Test
//    public void testBulkProcessor() throws Exception {
//        BulkProcessor.Listener listener = new BulkProcessor.Listener() {
//            @Override
//            public void beforeBulk(long executionId, BulkRequest request) {
//
//            }
//
//            @Override
//            public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
//
//            }
//
//            @Override
//            public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
//
//            }
//        };
//
//        BulkProcessor bulkProcessor = BulkProcessor.builder(
//                (request, bulkListener) -> client.bulkAsync(request, RequestOptions.DEFAULT, bulkListener),
//                listener)
//                .setConcurrentRequests(0)
//                .setBulkActions(10)
//                .build();
//
//    }
}