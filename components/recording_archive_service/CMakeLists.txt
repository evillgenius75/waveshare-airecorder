idf_component_register(
    SRCS "recording_archive_service.cpp"
    INCLUDE_DIRS "include"
    REQUIRES esp_timer nvs_flash recording_service storage_service task_config
    PRIV_REQUIRES espressif__cjson
)

target_compile_features(${COMPONENT_LIB} PUBLIC cxx_std_17)
