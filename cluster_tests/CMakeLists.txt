ADD_CUSTOM_TARGET(cluster_tests_python_requirements
        COMMAND ${PYTHON_EXE} -m ensurepip
        COMMAND ${PYTHON_EXE} -m pip
        install -r ${CMAKE_CURRENT_SOURCE_DIR}/requirements.txt)
SEPARATE_ARGUMENTS(cluster_test_args NATIVE_COMMAND $ENV{CLUSTER_TESTS_ARGS})
ADD_CUSTOM_TARGET(cluster_tests
        COMMAND JAVA_HOME=${Java_JAVA_HOME} ${PYTHON_EXE}
        ${CMAKE_CURRENT_SOURCE_DIR}/run.py ${cluster_test_args}
        DEPENDS cluster_tests_python_requirements
        COMMAND_EXPAND_LISTS)
# Retain old name as an alias to allow the existing jenkins jobs to work
ADD_CUSTOM_TARGET(api_test DEPENDS cluster_tests)

ADD_TEST(cluster_tests
        "${CMAKE_COMMAND}"
        --build "${CMAKE_BINARY_DIR}"
        --target cluster_tests)
