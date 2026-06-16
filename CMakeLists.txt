# For more information about build system see
# https://docs.espressif.com/projects/esp-idf/en/latest/api-guides/build-system.html
# The following five lines of boilerplate have to be in your project's
# CMakeLists in this exact order for cmake to work correctly
cmake_minimum_required(VERSION 3.16)

include($ENV{IDF_PATH}/tools/cmake/project.cmake)

# --- 自定义版本号生成逻辑开始 ---
string(TIMESTAMP CURRENT_DATE "%Y%m%d")
execute_process(
    COMMAND git rev-parse --short HEAD
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
    OUTPUT_VARIABLE GIT_HASH
    OUTPUT_STRIP_TRAILING_WHITESPACE
    ERROR_QUIET
)
if(NOT GIT_HASH)
    set(GIT_HASH "unknown")
endif()
set(PROJECT_VER "1.0.${CURRENT_DATE}-${GIT_HASH}")
add_compile_definitions(PROJECT_VER=\"${PROJECT_VER}\")
# --- 自定义版本号生成逻辑结束 ---

project(esp32s3-remote-display)

# 编译完成后自动拷贝出一份带版本号的固件，专供 OTA 升级使用 (修复拷贝报错)
add_custom_target(versioned_ota_bin ALL
    COMMAND ${CMAKE_COMMAND} -E copy
        "${CMAKE_BINARY_DIR}/${PROJECT_NAME}.bin"
        "${CMAKE_BINARY_DIR}/${PROJECT_NAME}-${PROJECT_VER}.bin"
    DEPENDS gen_project_binary
    COMMENT "Generated versioned OTA binary: ${PROJECT_NAME}-${PROJECT_VER}.bin"
)

# 编译完成后自动将 Bootloader、分区表和 App 合并为一个完整的 bin 文件，供一键烧录使用
add_custom_target(merged_bin ALL
    COMMAND python -m esptool --chip esp32s3 merge_bin -o "${PROJECT_NAME}-full-${PROJECT_VER}.bin" @flash_args
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
    DEPENDS gen_project_binary bootloader partition-table
    COMMENT "Generated merged firmware: ${PROJECT_NAME}-full-${PROJECT_VER}.bin"
)