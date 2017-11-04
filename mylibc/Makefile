CC = gcc
CFLAGS = -O0 -fPIC -Wall -g
LUADIR = /usr/maidong/skynet/3rd/lua
TARGET = package.so
SKYNETDIR = /usr/maidong/skynet/skynet-src/
ZOOKEEPER = /usr/local/include/zookeeper/
ZNODE = ./znode

.PHONY : all clean

all : $(TARGET) smq.so rdkafka.so znode.so mytest

$(TARGET) : package_proto.c
	$(CC) $(CFLAGS) -shared -o $@ -I../.. -I$(LUADIR) -I$(SKYNETDIR)  $^
	cp $(TARGET) /usr/maidong/skynet/luaclib/

smq.so : libmq.c
	$(CC) $(CFLAGS) -shared -o $@ -I../.. -I$(LUADIR) -I$(SKYNETDIR)  $^
	cp smq.so /usr/maidong/skynet/luaclib/

rdkafka.so : librdkafka.c
	$(CC) $(CFLAGS) -shared -o $@ -I../.. -I$(LUADIR) -I$(SKYNETDIR) -lrdkafka -lz -lpthread -lrt $^
	cp rdkafka.so /usr/maidong/skynet/luaclib/

znode.so : libznode.c linked.c memory_trace.c safe_queue.c smemory.c znode.c znode_high.c
	$(CC) $(CFLAGS) -shared -o $@ -I../.. -I$(LUADIR) -I$(SKYNETDIR) -I$(ZOOKEEPER)  -DTHREADED -lzookeeper_mt $^
	cp znode.so /usr/maidong/skynet/luaclib/

mytest : mytest.c linked.c memory_trace.c safe_queue.c smemory.c znode.c znode_high.c
	$(CC) $(CFLAGS) -o $@ -I../.. -I$(LUADIR) -I$(SKYNETDIR) -I$(ZOOKEEPER)  -DTHREADED -lzookeeper_mt $^

clean :
	rm -f $(TARGET) smq.so rdkafka.so znode.so mytest
