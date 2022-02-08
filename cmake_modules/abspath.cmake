# Utility script to compute absolute path given any input
GET_FILENAME_COMPONENT (result "${dir}" REALPATH)
EXECUTE_PROCESS (COMMAND "${CMAKE_COMMAND}" -E echo "${result}")
