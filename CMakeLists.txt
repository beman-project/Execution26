# cmake-format: off
# /CMakeLists.txt -*-makefile-*-
# SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception
# cmake-format: on

cmake_minimum_required(VERSION 3.23)

project(beman_execution26 VERSION 0.0.0 LANGUAGES CXX)
set(TARGET_NAME execution26)
set(TARGET_PREFIX beman.${TARGET_NAME})
set(TARGET_LIBRARY beman_${TARGET_NAME})
set(TARGET_ALIAS beman::${TARGET_NAME})
set(TARGETS_EXPORT_NAME ${CMAKE_PROJECT_NAME}Targets)

set(CMAKE_CXX_STANDARD 26)

include(CTest)
if(BUILD_TESTING)
  add_subdirectory(src/beman/${TARGET_NAME}/tests)
  enable_testing()
endif()

add_subdirectory(src/beman/execution26)
add_subdirectory(examples)

include(GNUInstallDirs)

set(INSTALL_CONFIGDIR ${CMAKE_INSTALL_LIBDIR}/cmake)

include(CMakePackageConfigHelpers)

write_basic_package_version_file(
  ${CMAKE_CURRENT_BINARY_DIR}/${CMAKE_PROJECT_NAME}ConfigVersion.cmake
  VERSION ${PROJECT_VERSION}
  COMPATIBILITY AnyNewerVersion)

configure_package_config_file(
  "cmake/Config.cmake.in"
  ${CMAKE_CURRENT_BINARY_DIR}/${CMAKE_PROJECT_NAME}Config.cmake
  INSTALL_DESTINATION ${INSTALL_CONFIGDIR})

install(
  FILES ${CMAKE_CURRENT_BINARY_DIR}/${CMAKE_PROJECT_NAME}Config.cmake
        ${CMAKE_CURRENT_BINARY_DIR}/${CMAKE_PROJECT_NAME}ConfigVersion.cmake
  DESTINATION ${INSTALL_CONFIGDIR})
