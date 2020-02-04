package de.spinscale.restclient;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class ProductTests {

    @Test
    public void testObjectMapperToProduct() throws Exception {
        String json = "{" +
                "\"name\":\"Best name ever\"," +
                "\"description\":\"This is a wonderful description\"," +
                "\"stock_available\":123," +
                "\"price\":123.32" +
                "}";

        ObjectMapper mapper = new ObjectMapper();
        Product product = mapper.readValue(json, Product.class);
        assertThat(product.getId()).isNull();
        assertThat(product.getName()).isEqualTo("Best name ever");
        assertThat(product.getDescription()).isEqualTo("This is a wonderful description");
        assertThat(product.getPrice()).isEqualTo(123.32);
        assertThat(product.getStockAvailable()).isEqualTo(123);

        // now vice versa. serialize out again
        final String serializedJson = mapper.writeValueAsString(product);
        assertThat(serializedJson).isEqualTo(json);
    }
}
