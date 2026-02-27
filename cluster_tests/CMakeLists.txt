# The CV containers are the main users of the build target. They have `uv`
# which can manage python versions and our pip requirements for us.
ADD_CUSTOM_TARGET(cluster_tests
        COMMAND JAVA_HOME=${Java_JAVA_HOME} uv run --python 3.9.22
        --with-requirements ${CMAKE_CURRENT_SOURCE_DIR}/requirements.txt
        ${CMAKE_CURRENT_SOURCE_DIR}/run.py ${cluster_test_args}
        COMMAND_EXPAND_LISTS)
# Retain old name as an alias to allow the existing jenkins jobs to work
ADD_CUSTOM_TARGET(api_test DEPENDS cluster_tests)

ADD_TEST(cluster_tests
        "${CMAKE_COMMAND}"
        --build "${CMAKE_BINARY_DIR}"
        --target cluster_tests)
