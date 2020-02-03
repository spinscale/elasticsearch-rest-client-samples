package de.spinscale.restclient;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class ProductTests {

    @Test
    public void testObjectMapperToProduct() throws Exception {
        String json = "{\n" +
                "\"name\" : \"Best name ever\",\n" +
                "\"description\" : \"This is a wonderful description\",\n" +
                "\"price\" : 123.32,\n" +
                "\"stock_available\" : 123\n" +
                "}\n";

        ObjectMapper mapper = new ObjectMapper();
        Product product = mapper.readValue(json, Product.class);
        assertThat(product.getId()).isNull();
        assertThat(product.getName()).isEqualTo("Best name ever");
        assertThat(product.getDescription()).isEqualTo("This is a wonderful description");
        assertThat(product.getPrice()).isEqualTo(123.32);
        assertThat(product.getStockAvailable()).isEqualTo(123);
    }
}
