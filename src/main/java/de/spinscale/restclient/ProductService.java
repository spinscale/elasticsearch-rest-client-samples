package de.spinscale.restclient;

import java.io.IOException;

public interface ProductService {

    Product findById(String id) throws IOException;

    Page<Product> search(String query) throws IOException;

    Page<Product> next(Page page) throws IOException;

    void save(Product product) throws IOException;

}
