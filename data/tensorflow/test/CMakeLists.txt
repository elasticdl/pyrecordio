PROJECT(recordio_dataset_op_test)
CMAKE_MINIMUM_REQUIRED(VERSION 2.8) 

ADD_DEFINITIONS(-std=c++11)
SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fPIC")

EXECUTE_PROCESS(COMMAND python -c "import tensorflow as tf; print(tf.sysconfig.get_include(), end='')"
                OUTPUT_VARIABLE HEADER_DIR)
EXECUTE_PROCESS(COMMAND python -c "import tensorflow as tf; print(tf.sysconfig.get_lib(), end='')"
                OUTPUT_VARIABLE LINK_DIR)

AUX_SOURCE_DIRECTORY(. DIR_SRCS) 

INCLUDE_DIRECTORIES(${HEADER_DIR})
LINK_DIRECTORIES(${LINK_DIR})

INCLUDE_DIRECTORIES(/usr/include/gtest)
LINK_DIRECTORIES(/usr/local/lib/gtest)

ADD_EXECUTABLE(recordio_dataset_op_test ${DIR_SRCS} ../recordio)
TARGET_LINK_LIBRARIES(recordio_dataset_op_test libgtest.so libgtest_main.so libtensorflow_framework.so)

ENABLE_TESTING()
ADD_TEST(NAME recordio_dataset_op_test COMMAND recordio_dataset_op_test)
