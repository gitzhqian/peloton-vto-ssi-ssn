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
CMAKE_BINARY_DIR = /home/zq/15721-peloton/15721-peloton-hey-pull-from-here/ycsb_build

# Include any dependencies generated for this target.
include test/CMakeFiles/copy_test.dir/depend.make

# Include the progress variables for this target.
include test/CMakeFiles/copy_test.dir/progress.make

# Include the compile flags for this target's objects.
include test/CMakeFiles/copy_test.dir/flags.make

test/CMakeFiles/copy_test.dir/executor/copy_test.cpp.o: test/CMakeFiles/copy_test.dir/flags.make
test/CMakeFiles/copy_test.dir/executor/copy_test.cpp.o: ../test/executor/copy_test.cpp
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --progress-dir=/home/zq/15721-peloton/15721-peloton-hey-pull-from-here/ycsb_build/CMakeFiles --progress-num=$(CMAKE_PROGRESS_1) "Building CXX object test/CMakeFiles/copy_test.dir/executor/copy_test.cpp.o"
	cd /home/zq/15721-peloton/15721-peloton-hey-pull-from-here/ycsb_build/test && /usr/bin/c++  $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -o CMakeFiles/copy_test.dir/executor/copy_test.cpp.o -c /home/zq/15721-peloton/15721-peloton-hey-pull-from-here/test/executor/copy_test.cpp

test/CMakeFiles/copy_test.dir/executor/copy_test.cpp.i: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Preprocessing CXX source to CMakeFiles/copy_test.dir/executor/copy_test.cpp.i"
	cd /home/zq/15721-peloton/15721-peloton-hey-pull-from-here/ycsb_build/test && /usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -E /home/zq/15721-peloton/15721-peloton-hey-pull-from-here/test/executor/copy_test.cpp > CMakeFiles/copy_test.dir/executor/copy_test.cpp.i

test/CMakeFiles/copy_test.dir/executor/copy_test.cpp.s: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Compiling CXX source to assembly CMakeFiles/copy_test.dir/executor/copy_test.cpp.s"
	cd /home/zq/15721-peloton/15721-peloton-hey-pull-from-here/ycsb_build/test && /usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -S /home/zq/15721-peloton/15721-peloton-hey-pull-from-here/test/executor/copy_test.cpp -o CMakeFiles/copy_test.dir/executor/copy_test.cpp.s

test/CMakeFiles/copy_test.dir/executor/copy_test.cpp.o.requires:

.PHONY : test/CMakeFiles/copy_test.dir/executor/copy_test.cpp.o.requires

test/CMakeFiles/copy_test.dir/executor/copy_test.cpp.o.provides: test/CMakeFiles/copy_test.dir/executor/copy_test.cpp.o.requires
	$(MAKE) -f test/CMakeFiles/copy_test.dir/build.make test/CMakeFiles/copy_test.dir/executor/copy_test.cpp.o.provides.build
.PHONY : test/CMakeFiles/copy_test.dir/executor/copy_test.cpp.o.provides

test/CMakeFiles/copy_test.dir/executor/copy_test.cpp.o.provides.build: test/CMakeFiles/copy_test.dir/executor/copy_test.cpp.o


# Object files for target copy_test
copy_test_OBJECTS = \
"CMakeFiles/copy_test.dir/executor/copy_test.cpp.o"

# External object files for target copy_test
copy_test_EXTERNAL_OBJECTS =

test/copy_test: test/CMakeFiles/copy_test.dir/executor/copy_test.cpp.o
test/copy_test: test/CMakeFiles/copy_test.dir/build.make
test/copy_test: lib/libpeloton.so.0.0.5
test/copy_test: lib/libpeloton-test-common.a
test/copy_test: lib/libpeloton-proto.a
test/copy_test: /usr/lib/x86_64-linux-gnu/libboost_system.so
test/copy_test: /usr/lib/x86_64-linux-gnu/libboost_filesystem.so
test/copy_test: /usr/lib/x86_64-linux-gnu/libboost_thread.so
test/copy_test: /usr/lib/x86_64-linux-gnu/libboost_chrono.so
test/copy_test: /usr/lib/x86_64-linux-gnu/libboost_date_time.so
test/copy_test: /usr/lib/x86_64-linux-gnu/libboost_atomic.so
test/copy_test: /usr/lib/x86_64-linux-gnu/libpthread.so
test/copy_test: /usr/lib/x86_64-linux-gnu/libgflags.so
test/copy_test: /usr/local/lib/libprotobuf.so
test/copy_test: /usr/lib/x86_64-linux-gnu/libgflags.so
test/copy_test: /usr/local/lib/libprotobuf.so
test/copy_test: /usr/lib/x86_64-linux-gnu/libevent.so
test/copy_test: /usr/lib/x86_64-linux-gnu/libpqxx.so
test/copy_test: /usr/lib/x86_64-linux-gnu/libpq.so
test/copy_test: ../third_party/libpg_query/libpg_query.a
test/copy_test: test/CMakeFiles/copy_test.dir/link.txt
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --bold --progress-dir=/home/zq/15721-peloton/15721-peloton-hey-pull-from-here/ycsb_build/CMakeFiles --progress-num=$(CMAKE_PROGRESS_2) "Linking CXX executable copy_test"
	cd /home/zq/15721-peloton/15721-peloton-hey-pull-from-here/ycsb_build/test && $(CMAKE_COMMAND) -E cmake_link_script CMakeFiles/copy_test.dir/link.txt --verbose=$(VERBOSE)

# Rule to build all files generated by this target.
test/CMakeFiles/copy_test.dir/build: test/copy_test

.PHONY : test/CMakeFiles/copy_test.dir/build

test/CMakeFiles/copy_test.dir/requires: test/CMakeFiles/copy_test.dir/executor/copy_test.cpp.o.requires

.PHONY : test/CMakeFiles/copy_test.dir/requires

test/CMakeFiles/copy_test.dir/clean:
	cd /home/zq/15721-peloton/15721-peloton-hey-pull-from-here/ycsb_build/test && $(CMAKE_COMMAND) -P CMakeFiles/copy_test.dir/cmake_clean.cmake
.PHONY : test/CMakeFiles/copy_test.dir/clean

test/CMakeFiles/copy_test.dir/depend:
	cd /home/zq/15721-peloton/15721-peloton-hey-pull-from-here/ycsb_build && $(CMAKE_COMMAND) -E cmake_depends "Unix Makefiles" /home/zq/15721-peloton/15721-peloton-hey-pull-from-here /home/zq/15721-peloton/15721-peloton-hey-pull-from-here/test /home/zq/15721-peloton/15721-peloton-hey-pull-from-here/ycsb_build /home/zq/15721-peloton/15721-peloton-hey-pull-from-here/ycsb_build/test /home/zq/15721-peloton/15721-peloton-hey-pull-from-here/ycsb_build/test/CMakeFiles/copy_test.dir/DependInfo.cmake --color=$(COLOR)
.PHONY : test/CMakeFiles/copy_test.dir/depend

