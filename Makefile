CC = g++
AR = ar
RM = rm
INCLUDES = -I/home/dzl/rocksdb/include -I/home/dzl/treeline/include -I./include
CXXFLAGS = -g -Wall -std=c++17
LDLIBS = -L/home/dzl/rocksdb -L/home/dzl/treeline/build -L/home/dzl/treeline/build/_deps/crc32c-build -L/home/dzl/treeline/build/third_party/masstree -L/home/dzl/treeline/build/page_grouping -lrocksdb -lz -ldl -lpg_treeline -lmasstree -lpg -lcrc32c -pthread -lboost_serialization
ARFLAGS = rs

DIR_EXE = ./
DIR_LIB = ./
EXE = test_lsm2lix
LIB = liblsm2lix.a
EXE := $(addprefix $(DIR_EXE)/, $(EXE))
LIB := $(addprefix $(DIR_LIB)/, $(LIB))
SRCS = $(wildcard src/*.cc)
TEST_SRCS = $(wildcard test/*.cc)
OBJS = $(patsubst %.cc, %.o, $(SRCS))
TEST_OBJS = $(patsubst %.cc, %.o, $(TEST_SRCS))

all: $(EXE) #$(DEBUG)

$(EXE): $(OBJS) $(TEST_OBJS)
	$(CC) -o $@ $^ $(LIB_PATH) $(LDLIBS)
$(LIB): $(OBJS)
	$(AR) $(ARFLAGS) $@ $^
%.o: %.cc
	$(CC) -o $@ -c $^ $(INCLUDES) $(CXXFLAGS) 
clean:
	$(RM) $(OBJS) $(TEST_OBJS) $(EXE) $(LIB)