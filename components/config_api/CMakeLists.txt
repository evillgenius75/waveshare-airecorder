idf_component_register(
    SRCS "config_api.cpp" "config_api_http.cpp"
    INCLUDE_DIRS "include"
    REQUIRES espressif__cjson esp_http_server
    PRIV_REQUIRES esp_app_format
)

target_compile_features(${COMPONENT_LIB} PUBLIC cxx_std_17)
