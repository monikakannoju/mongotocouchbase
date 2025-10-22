package com.demo.util;

import org.bson.types.Binary;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.regex.Pattern;
@Component

public class DataTransformationUtil {
    
    private static final DateTimeFormatter DEFAULT_DATE_FORMATTER = 
        DateTimeFormatter.ISO_INSTANT.withZone(ZoneId.systemDefault());

    public static void convertMongoTypes(Map<String, Object> document) {
        if (document == null) return;
        
        for (Map.Entry<String, Object> entry : document.entrySet()) {
            Object value = entry.getValue();
            Object convertedValue = convertValue(value);
            
            if (convertedValue != value) {
                document.put(entry.getKey(), convertedValue);
            }
        }
    }

    private static Object convertValue(Object value) {
        if (value == null) {
            return null;
        }
        
        if (value instanceof Date) {
            // Convert Date to ISO string
            return DEFAULT_DATE_FORMATTER.format(((Date) value).toInstant());
        }
        else if (value instanceof ObjectId) {
            // Convert ObjectId to string
            return value.toString();
        }
        else if (value instanceof Binary) {
            // Convert Binary to Base64 string
            return Base64.getEncoder().encodeToString(((Binary) value).getData());
        }
        else if (value instanceof Decimal128) {
            // Convert Decimal128 to BigDecimal
            return ((Decimal128) value).bigDecimalValue();
        }
        else if (value instanceof Pattern) {
            // Convert RegEx Pattern to string
            return ((Pattern) value).pattern();
        }
        else if (value instanceof Map) {
            // Recursively process nested maps
            @SuppressWarnings("unchecked")
            Map<String, Object> mapValue = (Map<String, Object>) value;
            convertMongoTypes(mapValue);
            return mapValue;
        }
        else if (value instanceof List) {
            // Process lists that might contain convertible types
            List<Object> newList = new ArrayList<>();
            for (Object item : (List<?>) value) {
                newList.add(convertValue(item));
            }
            return newList;
        }
        else if (value instanceof Iterable) {
            // Handle other iterable types
            List<Object> newList = new ArrayList<>();
            for (Object item : (Iterable<?>) value) {
                newList.add(convertValue(item));
            }
            return newList;
        }
        
        // Return unchanged if no conversion needed
        return value;
    }
}
