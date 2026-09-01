package com.ecommeerce.inventory_service.service;

import com.ecommeerce.inventory_service.entity.Product;
import com.ecommeerce.inventory_service.repository.ProductRepository;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class ProductService {

    private final ProductRepository productRepository;

    public ProductService(ProductRepository productRepository) {
        this.productRepository = productRepository;
    }

    public Product addProduct(Product product) {
        return productRepository.save(product);
    }

    public List<Product> getAllProducts() {
        return productRepository.findAll();
    }

    public Product getProductById(Long id) {

        return productRepository.findById(id)
                .orElseThrow(() ->
                        new RuntimeException("Product not found with id: " + id));
    }

    public Product updateProduct(Long id, Product product) {

        Product existingProduct = getProductById(id);

        existingProduct.setName(product.getName());
        existingProduct.setQuantity(product.getQuantity());

        return productRepository.save(existingProduct);
    }

    public void deleteProduct(Long id) {

        Product existingProduct = getProductById(id);

        productRepository.delete(existingProduct);
    }
    public boolean reserveStock(Long productId, Integer requestedQuantity) {

        Product product = getProductById(productId);

        System.out.println("=================================");
        System.out.println("CHECKING REAL DATABASE STOCK");
        System.out.println("=================================");

        System.out.println(
                "Product ID: " + productId
        );

        System.out.println(
                "Available Stock: " + product.getQuantity()
        );

        System.out.println(
                "Requested Quantity: " + requestedQuantity
        );

        // Check stock
        if (product.getQuantity() < requestedQuantity) {

            System.out.println(
                    "❌ INSUFFICIENT STOCK"
            );

            return false;
        }

        // Reserve stock
        product.setQuantity(
                product.getQuantity() - requestedQuantity
        );

        productRepository.save(product);

        System.out.println(
                "✅ STOCK RESERVED"
        );

        System.out.println(
                "Remaining Stock: " + product.getQuantity()
        );

        return true;
    }
    public boolean releaseStock(
            Long productId,
            Integer quantity) {

        Product product = getProductById(productId);

        System.out.println("=================================");
        System.out.println("RELEASING INVENTORY");
        System.out.println("=================================");

        System.out.println(
                "Product ID: " + productId
        );

        System.out.println(
                "Current Stock: " + product.getQuantity()
        );

        System.out.println(
                "Release Quantity: " + quantity
        );

        product.setQuantity(
                product.getQuantity() + quantity
        );

        productRepository.save(product);

        System.out.println(
                "✅ INVENTORY RELEASED"
        );

        System.out.println(
                "New Stock: " + product.getQuantity()
        );

        return true;
    }
}
