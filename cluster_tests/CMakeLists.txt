
# Fetch WORKSPACE from ENV for Jenkins
SET(WORKSPACE ${CMAKE_CURRENT_BINARY_DIR})
IF ($ENV{WORKSPACE})
  SET(WORKSPACE $ENV{WORKSPACE})
ENDIF ($ENV{WORKSPACE})
# Fetch WORKSPACE from ENV for Jenkins
SET(VIRTUALENV_NAME ns_server_cluster_tests})
IF ($ENV{VIRTUALENV_NAME})
  SET(VIRTUALENV_NAME $ENV{VIRTUALENV_NAME})
ENDIF ($ENV{VIRTUALENV_NAME})

ADD_CUSTOM_TARGET(cluster_tests_python_requirements
        COMMAND python -m venv ${WORKSPACE}/${VIRTUALENV_NAME}
        COMMAND source ${WORKSPACE}/${VIRTUALENV_NAME}/bin/activate
        COMMAND python -m pip install --upgrade pip
        COMMAND python -m pip
        install -r ${CMAKE_CURRENT_SOURCE_DIR}/requirements.txt)
SEPARATE_ARGUMENTS(cluster_test_args NATIVE_COMMAND $ENV{CLUSTER_TESTS_ARGS})
ADD_CUSTOM_TARGET(cluster_tests
        COMMAND source ${WORKSPACE}/${VIRTUALENV_NAME}/bin/activate
        COMMAND JAVA_HOME=${Java_JAVA_HOME} python
        ${CMAKE_CURRENT_SOURCE_DIR}/run.py ${cluster_test_args}
        DEPENDS cluster_tests_python_requirements
        COMMAND_EXPAND_LISTS)
# Retain old name as an alias to allow the existing jenkins jobs to work
ADD_CUSTOM_TARGET(api_test DEPENDS cluster_tests)

ADD_TEST(cluster_tests
        "${CMAKE_COMMAND}"
        --build "${CMAKE_BINARY_DIR}"
        --target cluster_tests)
