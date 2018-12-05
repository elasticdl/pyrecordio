PROJECT(recordio)
CMAKE_MINIMUM_REQUIRED(VERSION 2.8) 

ADD_DEFINITIONS(-std=c++11)
SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -shared -fPIC -Xlinker --unresolved-symbols=ignore-in-shared-libs -O2")

EXECUTE_PROCESS(COMMAND python -c "import tensorflow as tf; print(tf.sysconfig.get_include(), end='')"
                OUTPUT_VARIABLE HEADER_DIR)
EXECUTE_PROCESS(COMMAND python -c "import tensorflow as tf; print(tf.sysconfig.get_lib(), end='')"
                OUTPUT_VARIABLE LINK_DIR)

AUX_SOURCE_DIRECTORY(. DIR_SRCS)

INCLUDE_DIRECTORIES(${HEADER_DIR})
LINK_DIRECTORIES(${LINK_DIR})

ADD_LIBRARY(recordio SHARED ${DIR_SRCS})
TARGET_LINK_LIBRARIES(recordio libtensorflow_framework.so)
