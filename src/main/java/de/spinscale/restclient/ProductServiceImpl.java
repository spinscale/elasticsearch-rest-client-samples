package de.spinscale.restclient;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.GetResponse;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

// use async in a real application
public class ProductServiceImpl implements ProductService {

    private final String index;
    private final ElasticsearchClient client;

    public ProductServiceImpl(String index, ElasticsearchClient client) {
        this.index = index;
        this.client = client;
    }

    @Override
    public Product findById(String id) throws IOException {
        final GetResponse<Product> getResponse = client.get(builder -> builder.index(index).id(id), Product.class);
        Product product = getResponse.source();
        product.setId(id);
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
        final SearchResponse<Product> response = client.search(searchRequest, Product.class);
        if (response.hits().total().value() == 0) {
            return Page.EMPTY;
        }
        if (response.hits().hits().isEmpty()) {
            return Page.EMPTY;
        }

        response.hits().hits().forEach(hit -> hit.source().setId(hit.id()));
        final List<Product> products = response.hits().hits().stream().map(Hit::source).collect(Collectors.toList());
        return new Page(products, input, searchRequest.from(), searchRequest.size());
    }

    private SearchRequest createSearchRequest(String input, int from, int size) {
        return new SearchRequest.Builder()
                .from(from)
                .size(size)
                .query(qb -> qb.multiMatch(mmqb -> mmqb.query(input).fields("name", "description")))
                .build();
    }

    @Override
    public void save(Product product) throws IOException {
        save(Collections.singletonList(product));
    }

    public void save(List<Product> products) throws IOException {
        final BulkResponse response = client.bulk(builder -> {
            for (Product product : products) {
                builder.index(index)
                       .operations(ob -> {
                           if (product.getId() != null) {
                               ob.index(ib -> ib.document(product).id(product.getId()));
                           } else {
                               ob.index(ib -> ib.document(product));
                           }
                           return ob;
                       });
            }
            return builder;
        });
        // TODO: Why does the response item not include the ID being returned?!
        // TODO: add it back here
    }
}
