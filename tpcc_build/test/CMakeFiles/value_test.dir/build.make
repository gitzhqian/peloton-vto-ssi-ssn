# CMAKE generated file: DO NOT EDIT!
# Generated by "Unix Makefiles" Generator, CMake Version 3.10

# Delete rule output on recipe failure.
.DELETE_ON_ERROR:


#=============================================================================
# Special targets provided by cmake.

# Disable implicit rules so canonical targets will work.
.SUFFIXES:


# Remove some rules from gmake that .SUFFIXES does not remove.
SUFFIXES =

.SUFFIXES: .hpux_make_needs_suffix_list


# Suppress display of executed commands.
$(VERBOSE).SILENT:


# A target that is always out of date.
cmake_force:

.PHONY : cmake_force

#=============================================================================
# Set environment variables for the build.

# The shell in which to execute make rules.
SHELL = /bin/sh

# The CMake executable.
CMAKE_COMMAND = /usr/bin/cmake

# The command to remove a file.
RM = /usr/bin/cmake -E remove -f

# Escaping for special characters.
EQUALS = =

# The top-level source directory on which CMake was run.
CMAKE_SOURCE_DIR = /home/zq/15721-peloton/15721-peloton-hey-pull-from-here

# The top-level build directory on which CMake was run.
CMAKE_BINARY_DIR = /home/zq/15721-peloton/15721-peloton-hey-pull-from-here/tpcc_build

# Include any dependencies generated for this target.
include test/CMakeFiles/value_test.dir/depend.make

# Include the progress variables for this target.
include test/CMakeFiles/value_test.dir/progress.make

# Include the compile flags for this target's objects.
include test/CMakeFiles/value_test.dir/flags.make

test/CMakeFiles/value_test.dir/type/value_test.cpp.o: test/CMakeFiles/value_test.dir/flags.make
test/CMakeFiles/value_test.dir/type/value_test.cpp.o: ../test/type/value_test.cpp
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --progress-dir=/home/zq/15721-peloton/15721-peloton-hey-pull-from-here/tpcc_build/CMakeFiles --progress-num=$(CMAKE_PROGRESS_1) "Building CXX object test/CMakeFiles/value_test.dir/type/value_test.cpp.o"
	cd /home/zq/15721-peloton/15721-peloton-hey-pull-from-here/tpcc_build/test && /usr/bin/c++  $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -o CMakeFiles/value_test.dir/type/value_test.cpp.o -c /home/zq/15721-peloton/15721-peloton-hey-pull-from-here/test/type/value_test.cpp

test/CMakeFiles/value_test.dir/type/value_test.cpp.i: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Preprocessing CXX source to CMakeFiles/value_test.dir/type/value_test.cpp.i"
	cd /home/zq/15721-peloton/15721-peloton-hey-pull-from-here/tpcc_build/test && /usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -E /home/zq/15721-peloton/15721-peloton-hey-pull-from-here/test/type/value_test.cpp > CMakeFiles/value_test.dir/type/value_test.cpp.i

test/CMakeFiles/value_test.dir/type/value_test.cpp.s: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Compiling CXX source to assembly CMakeFiles/value_test.dir/type/value_test.cpp.s"
	cd /home/zq/15721-peloton/15721-peloton-hey-pull-from-here/tpcc_build/test && /usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -S /home/zq/15721-peloton/15721-peloton-hey-pull-from-here/test/type/value_test.cpp -o CMakeFiles/value_test.dir/type/value_test.cpp.s

test/CMakeFiles/value_test.dir/type/value_test.cpp.o.requires:

.PHONY : test/CMakeFiles/value_test.dir/type/value_test.cpp.o.requires

test/CMakeFiles/value_test.dir/type/value_test.cpp.o.provides: test/CMakeFiles/value_test.dir/type/value_test.cpp.o.requires
	$(MAKE) -f test/CMakeFiles/value_test.dir/build.make test/CMakeFiles/value_test.dir/type/value_test.cpp.o.provides.build
.PHONY : test/CMakeFiles/value_test.dir/type/value_test.cpp.o.provides

test/CMakeFiles/value_test.dir/type/value_test.cpp.o.provides.build: test/CMakeFiles/value_test.dir/type/value_test.cpp.o


# Object files for target value_test
value_test_OBJECTS = \
"CMakeFiles/value_test.dir/type/value_test.cpp.o"

# External object files for target value_test
value_test_EXTERNAL_OBJECTS =

test/value_test: test/CMakeFiles/value_test.dir/type/value_test.cpp.o
test/value_test: test/CMakeFiles/value_test.dir/build.make
test/value_test: lib/libpeloton.so.0.0.5
test/value_test: lib/libpeloton-test-common.a
test/value_test: lib/libpeloton-proto.a
test/value_test: /usr/lib/x86_64-linux-gnu/libboost_system.so
test/value_test: /usr/lib/x86_64-linux-gnu/libboost_filesystem.so
test/value_test: /usr/lib/x86_64-linux-gnu/libboost_thread.so
test/value_test: /usr/lib/x86_64-linux-gnu/libboost_chrono.so
test/value_test: /usr/lib/x86_64-linux-gnu/libboost_date_time.so
test/value_test: /usr/lib/x86_64-linux-gnu/libboost_atomic.so
test/value_test: /usr/lib/x86_64-linux-gnu/libpthread.so
test/value_test: /usr/lib/x86_64-linux-gnu/libgflags.so
test/value_test: /usr/local/lib/libprotobuf.so
test/value_test: /usr/lib/x86_64-linux-gnu/libgflags.so
test/value_test: /usr/local/lib/libprotobuf.so
test/value_test: /usr/lib/x86_64-linux-gnu/libevent.so
test/value_test: /usr/lib/x86_64-linux-gnu/libpqxx.so
test/value_test: /usr/lib/x86_64-linux-gnu/libpq.so
test/value_test: ../third_party/libpg_query/libpg_query.a
test/value_test: test/CMakeFiles/value_test.dir/link.txt
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --bold --progress-dir=/home/zq/15721-peloton/15721-peloton-hey-pull-from-here/tpcc_build/CMakeFiles --progress-num=$(CMAKE_PROGRESS_2) "Linking CXX executable value_test"
	cd /home/zq/15721-peloton/15721-peloton-hey-pull-from-here/tpcc_build/test && $(CMAKE_COMMAND) -E cmake_link_script CMakeFiles/value_test.dir/link.txt --verbose=$(VERBOSE)

# Rule to build all files generated by this target.
test/CMakeFiles/value_test.dir/build: test/value_test

.PHONY : test/CMakeFiles/value_test.dir/build

test/CMakeFiles/value_test.dir/requires: test/CMakeFiles/value_test.dir/type/value_test.cpp.o.requires

.PHONY : test/CMakeFiles/value_test.dir/requires

test/CMakeFiles/value_test.dir/clean:
	cd /home/zq/15721-peloton/15721-peloton-hey-pull-from-here/tpcc_build/test && $(CMAKE_COMMAND) -P CMakeFiles/value_test.dir/cmake_clean.cmake
.PHONY : test/CMakeFiles/value_test.dir/clean

test/CMakeFiles/value_test.dir/depend:
	cd /home/zq/15721-peloton/15721-peloton-hey-pull-from-here/tpcc_build && $(CMAKE_COMMAND) -E cmake_depends "Unix Makefiles" /home/zq/15721-peloton/15721-peloton-hey-pull-from-here /home/zq/15721-peloton/15721-peloton-hey-pull-from-here/test /home/zq/15721-peloton/15721-peloton-hey-pull-from-here/tpcc_build /home/zq/15721-peloton/15721-peloton-hey-pull-from-here/tpcc_build/test /home/zq/15721-peloton/15721-peloton-hey-pull-from-here/tpcc_build/test/CMakeFiles/value_test.dir/DependInfo.cmake --color=$(COLOR)
.PHONY : test/CMakeFiles/value_test.dir/depend

