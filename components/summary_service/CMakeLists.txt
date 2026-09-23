idf_component_register(
    SRCS "summary_service.cpp"
    INCLUDE_DIRS "include"
    REQUIRES gemini_service recording_archive_service recording_service storage_service task_config
    PRIV_REQUIRES espressif__cjson
)

target_compile_features(${COMPONENT_LIB} PUBLIC cxx_std_17)
