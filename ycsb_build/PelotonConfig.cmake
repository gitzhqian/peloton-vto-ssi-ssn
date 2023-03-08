# Config file for the Peloton package.
#
# After successful configuration the following variables
# will be defined:
#
#   Peloton_INCLUDE_DIRS - Peloton include directories
#   Peloton_LIBRARIES    - libraries to link against
#   Peloton_DEFINITIONS  - a list of definitions to pass to compiler
#

# Compute paths
get_filename_component(Peloton_CMAKE_DIR "${CMAKE_CURRENT_LIST_FILE}" PATH)
set(Peloton_INCLUDE_DIRS "/home/zq/15721-peloton/15721-peloton-hey-pull-from-here/src;/usr/include;/usr/local/include;/home/zq/15721-peloton/15721-peloton-hey-pull-from-here/ycsb_build/include;/usr/include/postgresql;/home/zq/15721-peloton/15721-peloton-hey-pull-from-here/src/include;/home/zq/15721-peloton/15721-peloton-hey-pull-from-here/test/include;/home/zq/15721-peloton/15721-peloton-hey-pull-from-here/third_party")



# Our library dependencies
if(NOT TARGET peloton AND NOT peloton_BINARY_DIR)
  include("${Peloton_CMAKE_DIR}/PelotonTargets.cmake")
endif()

# List of IMPORTED libs created by PelotonTargets.cmake
set(Peloton_LIBRARIES peloton)

# Definitions
set(Peloton_DEFINITIONS "")
