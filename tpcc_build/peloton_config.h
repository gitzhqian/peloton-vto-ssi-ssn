/* Sources directory */
#define SOURCE_FOLDER "/home/zq/15721-peloton/15721-peloton-hey-pull-from-here"

/* Binaries directory */
#define BINARY_FOLDER "/home/zq/15721-peloton/15721-peloton-hey-pull-from-here/tpcc_build"

/* Temporary (TODO: remove) */
#if 1
  #define CMAKE_SOURCE_DIR SOURCE_FOLDER "/src/"
  #define EXAMPLES_SOURCE_DIR BINARY_FOLDER "/examples/"
  #define CMAKE_EXT ".gen.cmake"
#else
  #define CMAKE_SOURCE_DIR "src/"
  #define EXAMPLES_SOURCE_DIR "examples/"
  #define CMAKE_EXT ""
#endif
