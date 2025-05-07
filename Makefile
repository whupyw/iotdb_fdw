TOOLPREFIX = /usr/bin/

CC = $(TOOLPREFIX)gcc
LD = $(TOOLPREFIX)gcc
AS = $(TOOLPREFIX)gcc
AR = $(TOOLPREFIX)ar
CXX = $(TOOLPREFIX)g++

# 编译选项
CFLAGS = -ggdb -Wall -Werror -O0 -fno-omit-frame-pointer 
CFLAGS += -I include
CFLAGS += -fpic
CFLAGS += -I ../../src/include/

CXXFLAGS = -ggdb -Wall -Werror -O0 -fno-omit-frame-pointer
CXXFLAGS += -I include
CXXFLAGS += -fpic
CXXFLAGS += -I ../../src/include/
CXXFLAGS += -I ./include/include/

# 链接选项
LDFLAGS = -shared -L$(PWD)/include/lib/ -Wl,-rpath,$(PWD)/include/lib/
LDLIBS  = -liotdb_session
LDLIBS += -lparse
LDLIBS += -lthrift
LDLIBS += -lthriftnb  
LDLIBS += -lthriftz
LDLIBS += -ltutorialgencpp

# 获取所有 .c 文件并生成对应的目标文件列表
cc_src = $(wildcard ./*.c)
cc_obj = $(patsubst %.c,%.o,$(cc_src))

cxx_src = $(wildcard ./*.cpp)
cxx_obj = $(patsubst %.cpp,%.o,$(cxx_src))

.PHONY: all clean

# 默认目标
all: iotdb_fdw.so
	@echo "iotdb_fdw.so is built successfully."

# 链接规则
iotdb_fdw.so: $(cc_obj) $(cxx_obj)
	$(CXX) $(LDFLAGS) -o $@ $^ $(LDLIBS)

# 编译规则
%.o: %.c
	$(CC) $(CFLAGS) -c -o $@ $<

%.o: %.cpp
	$(CXX) $(CXXFLAGS) -c -o $@ $<

# 清理规则
clean:
	rm -f $(cc_obj) $(cxx_obj) iotdb_fdw.so