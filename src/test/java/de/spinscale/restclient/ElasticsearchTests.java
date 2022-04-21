package de.spinscale.restclient;

import co.elastic.clients.elasticsearch.ElasticsearchAsyncClient;
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.FieldSort;
import co.elastic.clients.elasticsearch._types.HealthStatus;
import co.elastic.clients.elasticsearch._types.SortOrder;
import co.elastic.clients.elasticsearch._types.aggregations.HistogramAggregate;
import co.elastic.clients.elasticsearch.cluster.HealthResponse;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.elasticsearch.core.search.TotalHitsRelation;
import co.elastic.clients.json.JsonData;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import jakarta.json.Json;
import jakarta.json.JsonObject;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.Node;
import org.elasticsearch.client.NodeSelector;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;
import org.testcontainers.elasticsearch.ElasticsearchContainer;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

public class ElasticsearchTests {

    private static final String IMAGE_NAME = "docker.elastic.co/elasticsearch/elasticsearch:8.1.3";
    private static final ElasticsearchContainer container =
            new ElasticsearchContainer(IMAGE_NAME)
                    .withExposedPorts(9200)
                    .withPassword("s3cret");

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
    private static ElasticsearchAsyncClient asyncClient;

    @BeforeAll
    public static void startElasticsearchCreateLocalClient() throws Exception {
        // remove from environment to have TLS enabled
        container.getEnvMap().remove("xpack.security.enabled");

        // custom wait strategy not requiring any TLS tuning...
        container.setWaitStrategy(new LogMessageWaitStrategy().withRegEx(".*\"message\":\"started\".*"));
        container.start();

        // extract the ca cert from the running instance
        byte[] certAsBytes = container.copyFileFromContainer("/usr/share/elasticsearch/config/certs/http_ca.crt", InputStream::readAllBytes);

        HttpHost host = new HttpHost("localhost", container.getMappedPort(9200), "https");
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials("elastic", "s3cret"));
        final RestClientBuilder builder = RestClient.builder(host);

        builder.setHttpClientConfigCallback(clientBuilder -> {
            clientBuilder.setSSLContext(SslUtils.createContextFromCaCert(certAsBytes));
            clientBuilder.setDefaultCredentialsProvider(credentialsProvider);
            return clientBuilder;
        });
        builder.setNodeSelector(INGEST_NODE_SELECTOR);

        final ObjectMapper mapper = new ObjectMapper();
        mapper.setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE);
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);

        restClient = builder.build();
        ElasticsearchTransport transport = new RestClientTransport(restClient, new JacksonJsonpMapper(mapper));
        client = new ElasticsearchClient(transport);
        asyncClient = new ElasticsearchAsyncClient(transport);
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

        final HealthResponse response = client.cluster().health();
        // check for yellow or green cluster health
        assertThat(response.status()).isNotEqualTo(HealthStatus.Red);

        // TODO: add back one async health request request
        CountDownLatch latch = new CountDownLatch(1);
        asyncClient.cluster().health()
                .whenComplete((resp, throwable) -> {
                    assertThat(resp.status()).isNotEqualTo(HealthStatus.Red);
                    latch.countDown();
                });
        latch.await(10, TimeUnit.SECONDS);
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

        final SearchResponse<Void> response = client.search(b -> b
                .index(INDEX)
                .query(qb -> qb.match(mqb -> mqb.field("name").query(builder -> builder.stringValue("Name"))))
                .sort(sb -> sb.field(FieldSort.of(fs -> fs.field("price").order(SortOrder.Desc))))
            , Void.class);

        final List<String> ids = response.hits().hits().stream().map(Hit::id).collect(Collectors.toList());
        final List<String> sort = response.hits().hits().get(response.hits().hits().size() - 1).sort();

        // first search after
        final SearchResponse<Void> searchAfterResponse = client.search(b -> b
                        .index(INDEX)
                        .query(qb -> qb.match(mqb -> mqb.field("name").query(builder -> builder.stringValue("Name"))))
                        .sort(sb -> sb.field(FieldSort.of(fs -> fs.field("price").order(SortOrder.Desc))))
                        .searchAfter(sort)
                , Void.class);

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

        final SearchResponse<Void> response = client.search(b -> b.index(INDEX).query(q -> q.bool(builder ->
                builder
                        .must(m -> m.multiMatch(mmq -> mmq.query("Book")))
                        .should(s -> s.range(r -> r.field("price").lt(JsonData.of(100))))
                        .filter(f -> f.range(r -> r.field("stock_available").gt(JsonData.of(0))))
                        .filter(f -> f.range(r -> r.field("price").gt(JsonData.of(0))))
        )), Void.class);

        // exact hit count
        assertThat(response.hits().total().value()).isEqualTo(2);
        assertThat(response.hits().total().relation()).isEqualTo(TotalHitsRelation.Eq);

        // first hit should be 2010 edition due to its price and the above should clause
        final List<Hit<Void>> hits = response.hits().hits();
        assertThat(hits.get(0).id()).isEqualTo("book-world-records-2010");
        assertThat(hits.get(1).id()).isEqualTo("book-world-records-2020");
    }

    @Test
    public void testAggregationBuilder() throws Exception {
        final List<Product> products = createProducts(100);
        productService.save(products);
        client.indices().refresh(b -> b.index(INDEX));

        final SearchResponse<Void> response = client.search(builder -> builder.index(INDEX).size(0)
                        .aggregations("price_histo", aggBuilder ->
                                aggBuilder.histogram(histo -> histo.interval(10.0).field("price"))
                                        .aggregations("stock_average", a -> a.avg(avg -> avg.field("stock_available")))),
                Void.class);

        assertThat(response.hits().hits()).isEmpty();
        assertThat(response.aggregations()).hasSize(1);
        HistogramAggregate histogram = response.aggregations().get("price_histo").histogram();
        // prices go from 0-120, so we should have 12 buckets on an interval with 10
        assertThat(histogram.buckets().array()).hasSize(12);

        // also all the average stock should go up
        final List<Double> averages = histogram.buckets().array().stream()
                .map(b -> b.aggregations().get("stock_average").avg().value())
                .toList();

        // check that averages are monotonically increasing due to the data design in createProducts();
        for (int i = 1; i < averages.size(); i++) {
            double previousValue = averages.get(i - 1);
            double currentValue = averages.get(i);
            assertThat(currentValue).isGreaterThan(previousValue);
        }
    }
}
