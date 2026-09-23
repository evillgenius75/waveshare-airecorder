idf_component_register(
    SRCS "timezone_service.cpp"
    INCLUDE_DIRS "include"
    REQUIRES esp_http_server esp_timer nvs_flash power_service task_config
    PRIV_REQUIRES espressif__cjson
)

target_compile_features(${COMPONENT_LIB} PUBLIC cxx_std_17)
