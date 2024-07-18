package com.example;


import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import com.fasterxml.jackson.databind.JsonNode; 
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import java.util.Properties;

import java.util.HashMap;
import java.util.Map;
import java.util.Arrays;
import java.util.List;
import java.io.File;
import java.util.ArrayList;
import java.io.IOException;
import java.util.Properties;
import java.util.stream.Collectors;

public class ProductStreamApp {
    public static void main(String[] args) {
     try{
        ObjectMapper objectMapper = new ObjectMapper();
        List<JsonNode> products = objectMapper.readValue(new File("/home/ubuntu/product-stream-app/src/main/java/com/example/products.json"), new TypeReference<List<JsonNode>>() {});
        
         // Create a map to categorize products for quick lookup
        Map<String, List<JsonNode>> categoryToProductsMap = products.stream()
            .collect(Collectors.groupingBy(product -> {
                JsonNode titleNode = product.get("title");
                if (titleNode != null) {
                    JsonNode shortTitleNode = titleNode.get("shortTitle");
                    if (shortTitleNode != null) {
                        return shortTitleNode.asText();
                    }
                }
                return "";
            }));


        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "product-stream-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "ec2-13-201-59-31.ap-south-1.compute.amazonaws.com:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        StreamsBuilder builder = new StreamsBuilder();
             
        KStream<String, String> source = builder.stream("productevent");

        KStream<String, String> filteredStream = source.flatMapValues(value -> {
            List<String> matchingProductsJson = new ArrayList<>();
            String category = value.trim();
            if (categoryToProductsMap.containsKey(category)) {
                for (JsonNode product : categoryToProductsMap.get(category)) {
                    try {
                        matchingProductsJson.add(objectMapper.writeValueAsString(product));
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
            return matchingProductsJson;
        });
        
        filteredStream.to("similarproducts", Produced.with(Serdes.String(), Serdes.String()));


        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
     }catch (Exception e) {
        e.printStackTrace();
      }
    }
}