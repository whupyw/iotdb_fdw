TOOLPREFIX = /usr/bin/

CC = $(TOOLPREFIX)gcc
LD = $(TOOLPREFIX)gcc
AS = $(TOOLPREFIX)gcc
AR = $(TOOLPREFIX)ar

# 编译选项
CFLAGS = -ggdb -Wall -Werror -O0 -fno-omit-frame-pointer 
CFLAGS += -I include
CFLAGS += -fpic
CFLAGS += -I /usr/include
CFLAGS += -I ./include/
CFLAGS += -I ../../src/include/

# 链接选项
LDFLAGS = -shared

# 获取所有 .c 文件并生成对应的目标文件列表
cc_src = $(wildcard ./*.c)
cc_obj = $(patsubst %.c,%.o,$(cc_src))

.PHONY: all clean

# 默认目标
all: iotdb_fdw.so
	@echo "iotdb_fdw.so is built successfully."

# 链接规则
iotdb_fdw.so: $(cc_obj)
	$(LD) $(LDFLAGS) -o $@ $^ 

# 编译规则
%.o: %.c
	$(CC) $(CFLAGS) -c -o $@ $<

# 清理规则
clean:
	rm -f $(cc_obj) iotdb_fdw.so