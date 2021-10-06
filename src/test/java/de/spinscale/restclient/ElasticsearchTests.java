package de.spinscale.restclient;

import co.elastic.clients.base.RestClientTransport;
import co.elastic.clients.base.Transport;
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._core.SearchResponse;
import co.elastic.clients.elasticsearch._core.search.Hit;
import co.elastic.clients.elasticsearch._core.search.TotalHitsRelation;
import co.elastic.clients.elasticsearch._types.Health;
import co.elastic.clients.elasticsearch.cluster.HealthResponse;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.module.SimpleModule;
import jakarta.json.Json;
import jakarta.json.JsonObject;
import org.apache.http.HttpHost;
import org.elasticsearch.client.Node;
import org.elasticsearch.client.NodeSelector;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.elasticsearch.ElasticsearchContainer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

public class ElasticsearchTests {

    private static final ElasticsearchContainer container =
            new ElasticsearchContainer("docker.elastic.co/elasticsearch/elasticsearch:7.15.0").withExposedPorts(9200);

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
    private static ElasticsearchClient client;
    private static RestClient restClient;
    private static ProductServiceImpl productService;

    @BeforeAll
    public static void startElasticsearchCreateLocalClient() {
        container.start();

        HttpHost host = new HttpHost("localhost", container.getMappedPort(9200));
        final RestClientBuilder builder = RestClient.builder(host);
        builder.setNodeSelector(INGEST_NODE_SELECTOR);

        final ObjectMapper mapper = new ObjectMapper();
        mapper.setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE);
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);

        restClient = builder.build();
        Transport transport = new RestClientTransport(restClient, new JacksonJsonpMapper(mapper));
        client = new ElasticsearchClient(transport);
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
        restClient.close();
    }

    @AfterEach
    public void deleteProductIndex() throws Exception {
        client.indices().delete(b -> b.index(INDEX));
    }

    @Test
    public void testClusterVersion() throws Exception {
        // this just exists to index some data, so the index deletion does not fail
        productService.save(createProducts(1));

        final HealthResponse response = client.cluster().health(b -> b);
        // check for yellow or green cluster health
        assertThat(response.status()).isNotEqualTo(Health.Red);

        // TODO: add back one async health request request
//        CountDownLatch latch = new CountDownLatch(1);
//        latch.await(10, TimeUnit.SECONDS);
    }

    // TODO requires fixing of bulk response
//    @Test
//    public void indexProductWithoutId() throws Exception {
//        Product product = createProducts(1).get(0);
//        product.setId(null);
//        assertThat(product.getId()).isNull();
//
//        productService.save(product);
//
//        assertThat(product.getId()).isNotNull();
//    }

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
        client.indices().refresh(b -> b.index(INDEX));

        final Page<Product> page = productService.search("9");
        assertThat(page.get()).hasSize(1);
        assertThat(page.get()).first().extracting("id").isEqualTo("9");
    }

    @Test
    public void testPagination() throws Exception {
        productService.save(createProducts(21));
        client.indices().refresh(b -> b.index(INDEX));

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
        client.indices().refresh(b -> b.index(INDEX));

        // TODO sorting has left building?!
        final SearchResponse<Object> response = client.search(b -> b
                .index(INDEX)
                .query(qb -> qb.match(mqb -> mqb.field("name").query("Name")))
                // SortBuilders.fieldSort("price").order(SortOrder.DESC)
                .sort(), Object.class);

        final List<String> ids = response.hits().hits().stream().map(Hit::id).collect(Collectors.toList());
        final List<String> sort = response.hits().hits().get(response.hits().hits().size() - 1).sort();
        final JsonObject sortOrder = Json.createObjectBuilder(Map.of("price", Map.of("order", "desc"))).build();

        // first search after
        final SearchResponse<Object> searchAfterResponse = client.search(b -> b
                        .index(INDEX)
                        .query(qb -> qb.match(mqb -> mqb.field("name").query("Name")))
                        .sort(sortOrder)
                        .searchAfter(sort)
                , Object.class);

        final List<String> searchAfterIds = searchAfterResponse.hits().hits().stream().map(Hit::id).collect(Collectors.toList());

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
        client.indices().refresh(b -> b.index(INDEX));

//        BoolQuery query = new BoolQuery(builder ->
//                builder.addMust(qb -> qb.multiMatch(b -> b.query("Book")))
//                        .addShould(qb -> qb.range(Json.createObjectBuilder(Map.of("price", Map.of("lt", 100))).build()))
//                        .addFilter(qb -> qb.range(Json.createObjectBuilder(Map.of("stock_available", Map.of("gt", 0))).build()))
//                        .addFilter(qb -> qb.range(Json.createObjectBuilder(Map.of("price", Map.of("gt", 0))).build()))
//                );

        final SearchResponse<Object> response = client.search(b -> b.index(INDEX).query(q -> q.bool(builder ->
                        builder.addMust(qb -> qb.multiMatch(mmq -> mmq.query("Book")))
                                .addShould(qb -> qb.range(Json.createObjectBuilder(Map.of("price", Map.of("lt", 100))).build()))
                                .addFilter(qb -> qb.range(Json.createObjectBuilder(Map.of("stock_available", Map.of("gt", 0))).build()))
                                .addFilter(qb -> qb.range(Json.createObjectBuilder(Map.of("price", Map.of("gt", 0))).build()))
                )
        ), Object.class);

        // exact hit count
        assertThat(response.hits().total().value()).isEqualTo(2);
        assertThat(response.hits().total().relation()).isEqualTo(TotalHitsRelation.Eq);

        // first hit should be 2010 edition due to its price and the above should clause
        final List<Hit<Object>> hits = response.hits().hits();
        assertThat(hits.get(0).id()).isEqualTo("book-world-records-2010");
        assertThat(hits.get(1).id()).isEqualTo("book-world-records-2020");
    }

    @Test
    public void testAggregationBuilder() throws Exception {
        final List<Product> products = createProducts(100);
        productService.save(products);
        client.indices().refresh(b -> b.index(INDEX));

        final SearchResponse<Object> response = client.search(builder -> builder.index(INDEX).size(0)
                        .aggregations("price_histo", aggBuilder ->
                                aggBuilder.histogram(histo -> histo.interval(10.0).field("price"))
                                        .aggs("stock_average", a -> a.avg(avg -> avg.field("stock_available")))),
                Object.class);

//        searchRequest.source().aggregation(
//                AggregationBuilders.histogram("price_histo").interval(10).field("price")
//                     .subAggregation(AggregationBuilders.avg("stock_average").field("stock_available")));

        assertThat(response.hits().hits()).isEmpty();

        // TODO WAIT UNTIL AGGREGATION RESPONSES CAN BE PARSED?!
        assertThat(response.aggregations()).hasSize(1);
//        Histogram histogram = response.getAggregations().get("price_histo");
//        // prices go from 0-120, so we should have 12 buckets on an interval with 10
//        assertThat(histogram.getBuckets()).hasSize(12);
//        // also all the average stock should go up
//        List<Double> averages = histogram.getBuckets().stream().map((Histogram.Bucket b) -> {
//            final Avg average =  b.getAggregations().get("stock_average");
//            return average.getValue();
//        }).collect(Collectors.toList());
//
//        // check that averages are monotonically increasing due to the data design in createProducts();
//        for (int i = 1; i < averages.size(); i++) {
//            double previousValue = averages.get(i - 1);
//            double currentValue = averages.get(i);
//            assertThat(currentValue).isGreaterThan(previousValue);
//        }
    }
}
