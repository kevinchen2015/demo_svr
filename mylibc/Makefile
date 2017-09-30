CC = gcc
CFLAGS = -O2 -fPIC -Wall
LUADIR = /usr/maidong/skynet/3rd/lua
TARGET = package.so
SKYNETDIR = /usr/maidong/skynet/skynet-src/

.PHONY : all clean

all : $(TARGET) smq.so rdkafka.so

$(TARGET) : package_proto.c
	$(CC) $(CFLAGS) -shared -o $@ -I../.. -I$(LUADIR) -I$(SKYNETDIR)  $^
	cp $(TARGET) /usr/maidong/skynet/luaclib/

smq.so : libmq.c
	$(CC) $(CFLAGS) -shared -o $@ -I../.. -I$(LUADIR) -I$(SKYNETDIR)  $^
	cp smq.so /usr/maidong/skynet/luaclib/

rdkafka.so : librdkafka.c
	$(CC) $(CFLAGS) -shared -o $@ -I../.. -I$(LUADIR) -I$(SKYNETDIR) -lrdkafka -lpthread -lrt  $^
	cp rdkafka.so /usr/maidong/skynet/luaclib/

clean :
	rm -f $(TARGET) smq.so rdkafka.so
