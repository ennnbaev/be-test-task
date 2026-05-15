package com.example.eventapi.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.format.FormatterRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

import java.time.Instant;

@Configuration
public class WebConfig implements WebMvcConfigurer {

    @Override
    public void addFormatters(FormatterRegistry registry) {
        // Allows ISO-8601 strings (e.g. 2024-01-01T00:00:00Z) to be used
        // directly as Instant @RequestParam values without extra annotations.
        registry.addConverter(String.class, Instant.class, Instant::parse);
    }
}
