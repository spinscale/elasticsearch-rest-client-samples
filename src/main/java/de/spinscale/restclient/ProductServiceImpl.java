package de.spinscale.restclient;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

// use async in a real application
public class ProductServiceImpl implements ProductService {

    private static final ObjectMapper mapper = new ObjectMapper();

    private final String index;
    private final RestHighLevelClient client;

    public ProductServiceImpl(String index, RestHighLevelClient client) {
        this.index = index;
        this.client = client;
    }

    @Override
    public Product findById(String id) throws IOException {
        final GetResponse response = client.get(new GetRequest(index, id), RequestOptions.DEFAULT);
        final Product product = mapper.readValue(response.getSourceAsBytes(), Product.class);
        product.setId(response.getId());
        return product;
    }

    @Override
    public Page<Product> search(String input) throws IOException {
        return createPage(createSearchRequest(input, 0, 10), input);
    }

    @Override
    public Page<Product> next(Page page) throws IOException {
        int from = page.getFrom() + page.getSize();
        final SearchRequest request = createSearchRequest(page.getInput(), from, page.getSize());
        return createPage(request, page.getInput());
    }

    private Page<Product> createPage(SearchRequest searchRequest, String input) throws IOException {
        final SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
        if (response.getHits().getTotalHits().value == 0) {
            return Page.EMPTY;
        }
        if (response.getHits().getHits().length == 0) {
            return Page.EMPTY;
        }
        List<Product> products = new ArrayList<>(response.getHits().getHits().length);
        for (SearchHit hit : response.getHits().getHits()) {
            final Product product = mapper.readValue(hit.getSourceAsString(), Product.class);
            product.setId(hit.getId());
            products.add(product);
        }

        return new Page(products, input, searchRequest.source().from(), searchRequest.source().size());
    }

    private SearchRequest createSearchRequest(String input, int from, int size) {
        final SearchRequest searchRequest = new SearchRequest();
        final SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.from(from);
        searchSourceBuilder.size(size);
        searchSourceBuilder.query(QueryBuilders.multiMatchQuery(input, "name", "description"));
        searchRequest.source(searchSourceBuilder);

        return searchRequest;
    }

    @Override
    public void save(Product product) throws IOException {
        save(Collections.singletonList(product));
    }

    public void save(List<Product> products) throws IOException {
        BulkRequest request = new BulkRequest();
        for (Product product : products) {
            request.add(indexRequest(product));
        }
        final BulkResponse response = client.bulk(request, RequestOptions.DEFAULT);
        for (int i = 0; i < products.size(); i++) {
            products.get(i).setId(response.getItems()[i].getId());
        }
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
}
