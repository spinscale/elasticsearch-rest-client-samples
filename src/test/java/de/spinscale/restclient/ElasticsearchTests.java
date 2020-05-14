package de.spinscale.restclient;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpHost;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Node;
import org.elasticsearch.client.NodeSelector;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.core.CountRequest;
import org.elasticsearch.client.core.CountResponse;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.metrics.Avg;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.elasticsearch.ElasticsearchContainer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

public class ElasticsearchTests {

    private static final ElasticsearchContainer container =
            new ElasticsearchContainer("docker.elastic.co/elasticsearch/elasticsearch:7.6.2").withExposedPorts(9200);

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
    private static final String INDEX = "my_index";
    private static RestHighLevelClient client;
    private static ProductServiceImpl productService;
    private static final ObjectMapper mapper = new ObjectMapper();

    @BeforeAll
    public static void startElasticsearchCreateLocalClient() {
        container.start();

        HttpHost host = new HttpHost("localhost", container.getMappedPort(9200));
        final RestClientBuilder builder = RestClient.builder(host);
        builder.setNodeSelector(INGEST_NODE_SELECTOR);
        client = new RestHighLevelClient(builder);
        productService = new ProductServiceImpl(INDEX, client);
    }

//    @BeforeAll
//    public static void startElasticsearchCreateCloudClient() {
//        String cloudId = "";
//        String user = "elastic";
//        String password = "";
//
//        // basic auth, preemptive authentication
//        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
//        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(user, password));
//
//        final RestClientBuilder builder = RestClient.builder(cloudId);
//        builder.setHttpClientConfigCallback(b -> b.setDefaultCredentialsProvider(credentialsProvider));
//
//        client = new RestHighLevelClient(builder);
//        productService = new ProductServiceImpl(INDEX, client);
//    }

    @AfterAll
    public static void closeResources() throws Exception {
        client.close();
    }

    @BeforeEach
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

        product = productService.findById("0");
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
        searchAfterRequest.source()
                .query(QueryBuilders.matchQuery("name", "Name"))
                .sort(SortBuilders.fieldSort("price").order(SortOrder.DESC));
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

    @Test
    public void testBulkProcessor() throws Exception {
        final Map<Long, String> bulkMap = new HashMap<>();

        BulkProcessor.Listener listener = new BulkProcessor.Listener() {
            @Override
            public void beforeBulk(long executionId, BulkRequest request) {
                bulkMap.put(executionId, "BEFORE");
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
                bulkMap.put(executionId, "AFTER");
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                bulkMap.put(executionId, "EXCEPTION");
            }
        };

        try (BulkProcessor bulkProcessor = BulkProcessor.builder(
                (request, bulkListener) -> client.bulkAsync(request, RequestOptions.DEFAULT, bulkListener),
                listener)
                .setConcurrentRequests(0)
                .setBulkActions(10)
                // extra long to see if it has been applied
                .setFlushInterval(TimeValue.timeValueDays(1))
                .build()) {

            final List<Product> products = createProducts(19);
            for (Product product : products) {
                bulkProcessor.add(indexRequest(product));
            }

            client.indices().refresh(new RefreshRequest(INDEX), RequestOptions.DEFAULT);

            // nine elements should still be in the bulk processor
            CountResponse countResponse = client.count(new CountRequest(INDEX), RequestOptions.DEFAULT);
            assertThat(countResponse.getCount()).isEqualTo(10);

            // lets flush out the remaining elements manually
            bulkProcessor.flush();

            client.indices().refresh(new RefreshRequest(INDEX), RequestOptions.DEFAULT);
            countResponse = client.count(new CountRequest(INDEX), RequestOptions.DEFAULT);
            assertThat(countResponse.getCount()).isEqualTo(19);

            assertThat(bulkMap).hasSize(2);
            assertThat(bulkMap).containsValues("AFTER");
            assertThat(bulkMap).doesNotContainValue("BEFORE");
            assertThat(bulkMap).doesNotContainValue("EXCEPTION");
        }

    }

    private IndexRequest indexRequest(Product product) throws IOException {
        final byte[] bytes = mapper.writeValueAsBytes(product);
        final IndexRequest request = new IndexRequest(INDEX);
        if (product.getId() != null) {
            request.id(product.getId());
        }
        request.source(bytes, XContentType.JSON);
        return request;
    }

    @Test
    public void testQueryBuilders() throws Exception {
        Product product1 = new Product();
        product1.setId("book-world-records-2020");
        product1.setStockAvailable(1);
        product1.setPrice(100);
        product1.setDescription("The book of the year!");
        product1.setName("Guinness book of records 2020");

        Product product2 = new Product();
        product2.setId("book-world-records-2010");
        product2.setStockAvailable(200);
        product2.setPrice(80);
        product2.setDescription("The book of the year!");
        product2.setName("Guinness book of records 2010");

        Product product3 = new Product();
        product3.setId("book-world-records-1890");
        product3.setStockAvailable(0);
        product3.setPrice(200);
        product3.setDescription("The book of the year!");
        product3.setName("Guinness book of records 1890");

        productService.save(Arrays.asList(product1, product2, product3));
        client.indices().refresh(new RefreshRequest(INDEX), RequestOptions.DEFAULT);

        final BoolQueryBuilder qb = QueryBuilders.boolQuery()
            .must(QueryBuilders.multiMatchQuery("Book"))
            .should(QueryBuilders.rangeQuery("price").lt(100))
            .filter(QueryBuilders.rangeQuery("stock_available").gt(0))
            .filter(QueryBuilders.rangeQuery("price").gt(0));

        SearchRequest searchRequest = new SearchRequest(INDEX);
        searchRequest.source().query(qb);
        final SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);

        // exact hit count
        assertThat(response.getHits().getTotalHits().value).isEqualTo(2);
        assertThat(response.getHits().getTotalHits().relation).isEqualTo(TotalHits.Relation.EQUAL_TO);

        // first hit should be 2010 edition due to its price and the above should clause
        final SearchHit[] hits = response.getHits().getHits();
        assertThat(hits[0].getId()).isEqualTo("book-world-records-2010");
        assertThat(hits[1].getId()).isEqualTo("book-world-records-2020");
    }

    @Test
    public void testAggregationBuilder() throws Exception {
        final List<Product> products = createProducts(100);
        productService.save(products);
        client.indices().refresh(new RefreshRequest(INDEX), RequestOptions.DEFAULT);

        SearchRequest searchRequest = new SearchRequest(INDEX);
        searchRequest.source().size(0);
        // let's find out if the higher priced items have more stock available
        searchRequest.source().aggregation(
                AggregationBuilders.histogram("price_histo").interval(10).field("price")
                     .subAggregation(AggregationBuilders.avg("stock_average").field("stock_available")));

        final SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
        assertThat(response.getHits().getHits()).isEmpty();
        Histogram histogram = response.getAggregations().get("price_histo");
        // prices go from 0-120, so we should have 12 buckets on an interval with 10
        assertThat(histogram.getBuckets()).hasSize(12);
        // also all the average stock should go up
        List<Double> averages = histogram.getBuckets().stream().map((Histogram.Bucket b) -> {
            final Avg average =  b.getAggregations().get("stock_average");
            return average.getValue();
        }).collect(Collectors.toList());

        // check that averages are monotonically increasing due to the data design in createProducts();
        for (int i = 1; i < averages.size(); i++) {
            double previousValue = averages.get(i - 1);
            double currentValue = averages.get(i);
            assertThat(currentValue).isGreaterThan(previousValue);
        }
    }
}
