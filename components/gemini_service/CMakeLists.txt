idf_component_register(
    SRCS "gemini_service.cpp"
    INCLUDE_DIRS "include"
    REQUIRES esp_http_client esp-tls esp_timer mbedtls nvs_flash recording_service task_config
    PRIV_REQUIRES config_api espressif__cjson
)

target_compile_features(${COMPONENT_LIB} PUBLIC cxx_std_17)
