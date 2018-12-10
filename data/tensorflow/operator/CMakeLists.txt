PROJECT(recordio_dataset_op)
CMAKE_MINIMUM_REQUIRED(VERSION 2.8) 

ADD_DEFINITIONS(-std=c++11)
SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -shared -fPIC -Xlinker --unresolved-symbols=ignore-in-shared-libs -O2")
SET(LOCAL_SOURCE_DIR ${CMAKE_SOURCE_DIR})
SET(LOCAL_LIB_DIR ${CMAKE_SOURCE_DIR}/recordio)

EXECUTE_PROCESS(COMMAND python3 -c "import tensorflow as tf; print(tf.sysconfig.get_include(), end='')"
                OUTPUT_VARIABLE HEADER_DIR)
EXECUTE_PROCESS(COMMAND python3 -c "import tensorflow as tf; print(tf.sysconfig.get_lib(), end='')"
                OUTPUT_VARIABLE LINK_DIR)

AUX_SOURCE_DIRECTORY(. DIR_SRCS)

INCLUDE_DIRECTORIES(${HEADER_DIR})
INCLUDE_DIRECTORIES(${LOCAL_SOURCE_DIR})
LINK_DIRECTORIES(${LINK_DIR})
LINK_DIRECTORIES(${LOCAL_LIB_DIR})

ADD_LIBRARY(recordio_dataset_op SHARED ${DIR_SRCS})
TARGET_LINK_LIBRARIES(recordio_dataset_op librecordio.so libtensorflow_framework.so)
