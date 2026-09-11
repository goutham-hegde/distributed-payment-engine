package com.dpe.orchestrator.admission;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties(AdmissionProperties.class)
public class AdmissionConfig {
}
